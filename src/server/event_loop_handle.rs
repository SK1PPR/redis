use mio::{Token, Waker};
use crate::RedisResponse;
use std::sync::mpsc::Sender;

#[derive(Debug)]
pub enum EventLoopMessage {
    UnblockClient { token: Token, response: RedisResponse },
    BlockClient { token: Token, timeout: u64 }
}


#[derive(Debug, Clone)]
pub struct EventLoopHandle {
    sender: Sender<EventLoopMessage>,
    waker: std::sync::Arc<Waker>
}

impl EventLoopHandle {

    pub fn new(sender: Sender<EventLoopMessage>, waker: std::sync::Arc<Waker>) -> Self {
        EventLoopHandle { sender, waker }
    }

    pub fn unblock_client(&self, token: Token, response: RedisResponse) {
        if let Err(e) = self.sender.send(EventLoopMessage::UnblockClient { token, response }) {
            log::error!("Failed to send UnblockClient message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn block_client(&self, token: Token, timeout: u64) {
        if let Err(e) = self.sender.send(EventLoopMessage::BlockClient { token, timeout }) {
            log::error!("Failed to send BlockClient message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }
}