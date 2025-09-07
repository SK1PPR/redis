use crate::RedisResponse;
use mio::{Token, Waker};
use std::sync::mpsc::Sender;

#[derive(Debug)]
pub enum EventLoopMessage {
    UnblockClient {
        token: Token,
        response: RedisResponse,
    },
    BlockClient {
        token: Token,
        timeout: u64,
    },
    StartMulti {
        token: Token,
    },
    ExecuteQueue {
        token: Token,
    },
    DiscardQueue {
        token: Token,
    },
    SendMessage {
        token: Token,
        channel: String,
        message: String,
    },
    SendFile {
        token: Token,
        contents: Vec<u8>
    }
}

#[derive(Debug, Clone)]
pub struct EventLoopHandle {
    sender: Sender<EventLoopMessage>,
    waker: std::sync::Arc<Waker>,
}

impl EventLoopHandle {
    pub fn new(sender: Sender<EventLoopMessage>, waker: std::sync::Arc<Waker>) -> Self {
        EventLoopHandle { sender, waker }
    }

    pub fn unblock_client(&self, token: Token, response: RedisResponse) {
        if let Err(e) = self
            .sender
            .send(EventLoopMessage::UnblockClient { token, response })
        {
            log::error!("Failed to send UnblockClient message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn block_client(&self, token: Token, timeout: u64) {
        if let Err(e) = self
            .sender
            .send(EventLoopMessage::BlockClient { token, timeout })
        {
            log::error!("Failed to send BlockClient message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn execute_queue(&self, token: Token) {
        if let Err(e) = self.sender.send(EventLoopMessage::ExecuteQueue { token }) {
            log::error!("Failed to send ExecuteQueue message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn discard_queue(&self, token: Token) {
        if let Err(e) = self.sender.send(EventLoopMessage::DiscardQueue { token }) {
            log::error!("Failed to send DiscardQueue message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn start_multi(&self, token: Token) {
        if let Err(e) = self.sender.send(EventLoopMessage::StartMulti { token }) {
            log::error!("Failed to send StartMulti message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn send_message(&self, token: Token, channel: String, message: String) {
        if let Err(e) = self.sender.send(EventLoopMessage::SendMessage {
            token,
            channel,
            message,
        }) {
            log::error!("Failed to send SendMessage message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }

    pub fn send_file(&self, token: Token, contents: Vec<u8>) {
        if let Err(e) = self.sender.send(EventLoopMessage::SendFile {
            token,
            contents,
        }) {
            log::error!("Failed to send SendFile message: {}", e);
            return;
        }

        if let Err(e) = self.waker.wake() {
            log::error!("Failed to wake event loop: {}", e);
        }
    }
}
