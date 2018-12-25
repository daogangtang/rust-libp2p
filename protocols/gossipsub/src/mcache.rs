use std::collections::hash_map::HashMap;
use message::Message;

pub struct MessageCache {
    msgs: LruCache<MessageId, Message>,
}

impl MessageCache {
   
    fn put(&self, msg: Message) -> Result<(), String> {


    }

    fn get(&self, msgid: MessageId) -> Result<Message, String> {


    }

    fn window(&self) -> Result<Vec<MessageId>, String> {


    }

    fn shift(&self) -> Result<(), String> {


    }


}
