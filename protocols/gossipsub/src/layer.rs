// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use cuckoofilter::CuckooFilter;
use futures::prelude::*;
use handler::GossipsubHandler;
use libp2p_core::swarm::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p_core::{protocols_handler::ProtocolsHandler, PeerId};
use protocol::{GossipsubMessage, GossipsubRpc, GossipsubSubscription, GossipsubSubscriptionAction,
                GossipsubControl};
use smallvec::SmallVec;
use std::{collections::VecDeque, iter, marker::PhantomData};
use std::collections::hash_map::{DefaultHasher, HashMap};
use tokio_io::{AsyncRead, AsyncWrite};
use topic::{Topic, TopicHash};

use rand;
use rand::seq::SliceRandom;
use rand::thread_rng;

pub type Mesh = HashMap<TopicHash, Vec<PeerId>>;


/// Network behaviour that automatically identifies nodes periodically, and returns information
/// about them.
pub struct Gossipsub<TSubstream> {
    /// Events that need to be yielded to the outside when polling.
    events: VecDeque<NetworkBehaviourAction<GossipsubRpc, GossipsubMessage>>,

    /// Peer id of the local node. Used for the source of the messages that we publish.
    local_peer_id: PeerId,

    /// List of peers the network is connected to, and the topics that they're subscribed to.
    // TODO: filter out peers that don't support gossipsub, so that we avoid hammering them with
    //       opened substreams
    connected_peers: HashMap<PeerId, SmallVec<[TopicHash; 8]>>,

    // List of topics we're subscribed to. Necessary to filter out messages that we receive
    // erroneously.
    subscribed_topics: SmallVec<[Topic; 16]>,

    // We keep track of the messages we received (in the format `hash(source ID, seq_no)`) so that
    // we don't dispatch the same message twice if we receive it twice on the network.
    received: CuckooFilter<DefaultHasher>,

    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,

    // the overlay meshes as a map of topics to lists of peers
    mesh: Mesh,

    // the overlay meshes as a map of topics to lists of peers
    gossip_mesh: Mesh,

    // the mesh peers to which we are publishing to without topic membership, as a map of topics to
    // lists of peers
    // fanout: Mesh,

    // protocol has but no need in Rust implementation, for it is the same as mcache
    // seen: MessageCache,

    // a message cache that contains the messages for the last few heartbeat ticks
    // mcache: MessageCache,

}

impl<TSubstream> Gossipsub<TSubstream> {
    /// Creates a `Gossipsub`.
    pub fn new(local_peer_id: PeerId) -> Self {
        Gossipsub {
            events: VecDeque::new(),
            local_peer_id,
            connected_peers: HashMap::new(),
            subscribed_topics: SmallVec::new(),
            received: CuckooFilter::new(),
            marker: PhantomData,
            mesh: Mesh::new(),
            gossip_mesh: Mesh::new(),
        }
    }
}

impl<TSubstream> Gossipsub<TSubstream> {
    /// Subscribes to a topic.
    ///
    /// Returns true if the subscription worked. Returns false if we were already subscribed.
    pub fn subscribe(&mut self, topic: Topic) -> bool {
        if self.subscribed_topics.iter().any(|t| t.hash() == topic.hash()) {
            return false;
        }

        for peer in self.connected_peers.keys() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.hash().clone(),
                        action: GossipsubSubscriptionAction::Subscribe,
                    }],
                    // TODO: send ControlGraft to other peers in the mesh overlay
                    controls: vec![GossipsubControl::Graft(topic)],
                },
            });
        }

        self.subscribed_topics.push(topic);

        true
    }

    /// Unsubscribes from a topic.
    ///
    /// Note that this only requires a `TopicHash` and not a full `Topic`.
    ///
    /// Returns true if we were subscribed to this topic.
    pub fn unsubscribe(&mut self, topic: impl AsRef<TopicHash>) -> bool {
        let topic = topic.as_ref();
        let pos = match self.subscribed_topics.iter().position(|t| t.hash() == topic) {
            Some(pos) => pos,
            None => return false
        };

        self.subscribed_topics.remove(pos);

        for peer in self.connected_peers.keys() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.clone(),
                        action: GossipsubSubscriptionAction::Unsubscribe,
                    }],
                    controls: vec![GossipsubControl::Prune(topic)],
                },
            });
        }

        true
    }

    /// Publishes a message to the network.
    ///
    /// > **Note**: Doesn't do anything if we're not subscribed to the topic.
    pub fn publish(&mut self, topic: impl Into<TopicHash>, data: impl Into<Vec<u8>>) {
        let message = GossipsubMessage {
            source: self.local_peer_id.clone(),
            data: data.into(),
            // If the sequence numbers are predictable, then an attacker could flood the network
            // with packets with the predetermined sequence numbers and absorb our legitimate
            // messages. We therefore use a random number.
            sequence_number: rand::random::<[u8; 20]>().to_vec(),
            topics: vec![topic],
        };

        // Don't publish the message if we're not subscribed ourselves to any of the topics.
        if !self.subscribed_topics.iter().any(|t| topic == t) {
            return;
        }

        self.received.add(&message);

        // check topic peer list in self.mesh if is empty, if empty try to collect it from
        // connected_peers
        let mut mesh_peers = match self.mesh.entry(topic).or_insert(vec![]);
        let mut gossip_mesh_peers = match self.gossip_mesh.entry(topic).or_insert(vec![]);
        if mesh_peers.len() == 0 {
            let _peers = Vec::new();
            for (peer_id, sub_topic) in self.connected_peers.iter() {
                if sub_topic.iter().any(|t| topic == t)) {
                    _peers.push(peer_id);
                }
            }

            // TODO: then random select some items from new_peers to form mesh peers
            // using TARGET_MESH_DEGREE, use shuffle here this need to introduce rand::seq::SliceRandom
            _peers.shuffle();
            let front_peers = &_peers[0..constants::TARGET_MESH_DEGREE];
            mesh_peers.extend(front_peers);
            let left_peers = &_peers[constants::TARGET_MESH_DEGREE..];
            gossip_mesh_peers.extend(left_peers);
            
            // TODO: when form mesh peers in this topic, we need to inform those peers to
            // execute ControlGraft to add this peer to its mesh overlay
            for peer_id in mesh_peers {
                self.events.push_back(NetworkBehaviourAction::SendEvent {
                    peer_id: peer_id.clone(),
                    event: GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: Vec::new(),
                        controls: vec![GossipsubControl::Graft(self.local_peer_id)]
                    }
                });
            }
        }

        // Send to peers we know are subscribed to the topic.
        for peer_id in mesh_peers.iter() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.clone(),
                event: GossipsubRpc {
                    subscriptions: Vec::new(),
                    messages: vec![message.clone()],
                    controls: Vec::new()
                }
            });
        }

        // TODO: then need to random select a subset of left peers in mesh
        // and send gossip msg to them, call them to request msg cache from me
        // current now do this in this stupid way, this can not reduce the payload of
        // network, we should place this to other chances such as heartbeat time to
        // make msg
        let gossip_send_peers = gossip_mesh_peers.choose_multiple(rng, constants::GOSSIP_MESH_LEN);

        // TODO: send gossip to rest peers subscribed this topic in gossip_mesh
        // IHave, with cached msg ids
        // calc these ids
        let cached_msg_ids = vec![];
        for peer_id in gossip_send_peers.iter() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.clone(),
                event: GossipsubRpc {
                    subscriptions: Vec::new(),
                    messages: Vec::new(),
                    controls: vec![GossipsubControl::IHave(cached_msg_ids)]
                }
            });
        }

    }

}

impl<TSubstream, TTopology> NetworkBehaviour<TTopology> for Gossipsub<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = GossipsubHandler<TSubstream>;
    type OutEvent = GossipsubMessage;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        GossipsubHandler::new()
    }

    fn inject_connected(&mut self, id: PeerId, _: ConnectedPoint) {
        // We need to send our subscriptions to the newly-connected node.
        for topic in self.subscribed_topics.iter() {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: id.clone(),
                event: GossipsubRpc {
                    messages: Vec::new(),
                    subscriptions: vec![GossipsubSubscription {
                        topic: topic.hash().clone(),
                        action: GossipsubSubscriptionAction::Subscribe,
                    }],
                },
            });
        }

        self.connected_peers.insert(id.clone(), SmallVec::new());
    }

    fn inject_disconnected(&mut self, id: &PeerId, _: ConnectedPoint) {
        let was_in = self.connected_peers.remove(id);
        debug_assert!(was_in.is_some());
    }

    fn inject_node_event(
        &mut self,
        propagation_source: PeerId,
        event: GossipsubRpc,
    ) {
        // Update connected peers topics
        for subscription in event.subscriptions {
            let mut remote_peer_topics = self.connected_peers
                .get_mut(&propagation_source)
                .expect("connected_peers is kept in sync with the peers we are connected to; we are guaranteed to only receive events from connected peers; QED");
            match subscription.action {
                GossipsubSubscriptionAction::Subscribe => {
                    if !remote_peer_topics.contains(&subscription.topic) {
                        remote_peer_topics.push(subscription.topic);
                    }
                }
                GossipsubSubscriptionAction::Unsubscribe => {
                    if let Some(pos) = remote_peer_topics.iter().position(|t| t == &subscription.topic ) {
                        remote_peer_topics.remove(pos);
                    }
                }
            }
        }

        // List of messages we're going to propagate on the network.
        let mut rpcs_to_dispatch: Vec<(PeerId, GossipsubRpc)> = Vec::new();

        for message in event.messages {
            // Use `self.received` to skip the messages that we have already received in the past.
            // Note that this can false positive.
            if !self.received.test_and_add(&message) {
                continue;
            }

            // Add the message to be dispatched to the user.
            if self.subscribed_topics.iter().any(|t| message.topics.iter().any(|u| t.hash() == u)) {
                self.events.push_back(NetworkBehaviourAction::GenerateEvent(message.clone()));
            }

            let topic = message.topics[0];
            // Propagate the message to everyone else who is subscribed to the topic in mesh
            // TODO: careful, not every msg we choose different random peers, this should only
            // be made once to form mesh content, if mesh has valid content, use it directly
            let mut mesh_peers = match self.mesh.entry(topic).or_insert(vec![]);
            let mut gossip_mesh_peers = match self.gossip_mesh.entry(topic).or_insert(vec![]);
            if mesh_peers.len() == 0 {
                let _peers = Vec::new();
                for (peer_id, sub_topic) in self.connected_peers.iter() {
                    if sub_topic.iter().any(|t| topic == t)) {
                        _peers.push(peer_id);
                    }
                }

                // TODO: then random select some items from new_peers to form mesh peers
                // using TARGET_MESH_DEGREE, use shuffle here this need to introduce rand::seq::SliceRandom
                _peers.shuffle();
                let front_peers = &_peers[0..constants::TARGET_MESH_DEGREE];
                mesh_peers.extend(front_peers);
                let left_peers = &_peers[constants::TARGET_MESH_DEGREE..];
                gossip_mesh_peers.extend(left_peers);

                // TODO: when form mesh peers in this topic, we need to inform those peers to
                // execute ControlGraft to add this peer to its mesh overlay
                for peer_id in mesh_peers {
                    self.events.push_back(NetworkBehaviourAction::SendEvent {
                        peer_id: peer_id.clone(),
                        event: GossipsubRpc {
                            subscriptions: Vec::new(),
                            messages: Vec::new(),
                            controls: vec![GossipsubControl::Graft(self.local_peer_id)]
                        }
                    });
                }
            }

            // add to message propagation vec
            for peer_id in mesh_peers.iter() {
                if peer_id == &propagation_source {
                    continue;
                }

                if let Some(pos) = rpcs_to_dispatch.iter().position(|(p, _)| p == peer_id) {
                    rpcs_to_dispatch[pos].1.messages.push(message.clone());
                } else {
                    rpcs_to_dispatch.push((peer_id.clone(), GossipsubRpc {
                        subscriptions: Vec::new(),
                        messages: vec![message.clone()],
                        controls: Vec::new(),
                    }));
                }
            }
        }

        // do propagating messages
        for (peer_id, rpc) in rpcs_to_dispatch {
            self.events.push_back(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id,
                event: rpc,
            });
        }

        // control msg process flow here
        for control_msg in event.controls {
            match control_msg {
                GossipsubControl::Graft(topic) => {
                    let mut topic_mesh = self.mesh.entry(topic).or_insert(vec![]);
                    topic_mesh.push(propagation_source);

                    let mut topic_gossip_mesh = match self.gossip_mesh.entry(topic).or_insert(vec![]);
                    if topic_gossip_mesh.len() == 0 {
                        // if no gossip mesh peers, calc it here
                        for (peer_id, sub_topic) in self.connected_peers.iter() {
                            if sub_topic.iter().any(|t| topic == t)) {
                                if !topic_mesh.iter().any(|p| peer_id == p) {
                                    topic_gossip_mesh.push(peer_id);
                                }
                            }
                        }
                    }
                    else if topic_gossip_mesh.len() > 0 {
                        match topic_gossip_mesh.iter().position(|p| p == propagation_source) {
                            Some(pos) => {
                                Some(topic_gossip_mesh.remove(pos))
                            },
                            None => None
                        };
                    }
                },
                GossipsubControl::Prune(topic) => {
                    let mut topic_mesh = self.mesh.get(topic);
                    if topic_mesh.is_some() {
                        let mut topic_mesh = topic_mesh.unwrap();
                        // XXX: error process
                        match topic_mesh.iter().position(|p| p == propagation_source) {
                            Some(pos) => {
                                let mut topic_gossip_mesh = match self.gossip_mesh.entry(topic).or_insert(vec![]);
                                topic_gossip_mesh.push(propagation_source);

                                Some(topic_mesh.remove(pos))
                            },
                            None => None
                        };

                    }
                },
                GossipsubControl::IHave((topic, msgids)) => {
                
                },
                GossipsubControl::IWant(msgids) => {
                
                },
            }

        }
    }

    fn poll(
        &mut self,
        _: &mut PollParameters<TTopology>,
    ) -> Async<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        if let Some(event) = self.events.pop_front() {
            return Async::Ready(event);
        }

        Async::NotReady
    }
}
