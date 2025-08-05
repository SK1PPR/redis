use super::{RedisCommand, RedisResponse};
use crate::storage::{MemoryStorage, Storage};

pub trait CommandExecutor {
    fn execute(&mut self, command: RedisCommand) -> RedisResponse;
}

pub struct RedisCommandExecutor {
    storage: MemoryStorage,
}

impl RedisCommandExecutor {
    pub fn new() -> Self {
        Self {
            storage: MemoryStorage::new(),
        }
    }
}

impl CommandExecutor for RedisCommandExecutor {
    fn execute(&mut self, command: RedisCommand) -> RedisResponse {
        log::debug!("Executing command: {:?}", command);
        
        match command {
            RedisCommand::Ping(message) => {
                match message {
                    Some(msg) => RedisResponse::BulkString(Some(msg)),
                    None => RedisResponse::pong(),
                }
            }
            RedisCommand::Echo(message) => {
                RedisResponse::BulkString(Some(message))
            }
            RedisCommand::Get(key) => {
                match self.storage.get(&key) {
                    Some(value) => RedisResponse::BulkString(Some(value)),
                    None => RedisResponse::nil(),
                }
            }
            RedisCommand::Set(key, value) => {
                self.storage.set(key, value);
                RedisResponse::ok()
            }
            RedisCommand::Del(keys) => {
                let deleted = self.storage.delete_multiple(keys);
                RedisResponse::Integer(deleted as i64)
            }
            RedisCommand::Exists(keys) => {
                let exists = self.storage.exists_multiple(&keys);
                RedisResponse::Integer(exists as i64)
            }
            RedisCommand::SetWithExpiry(key, value, expiry) => {
                self.storage.set_with_expiry(key, value, expiry);
                RedisResponse::ok()
            }
        }
    }
}

impl Default for RedisCommandExecutor {
    fn default() -> Self {
        Self::new()
    }
}