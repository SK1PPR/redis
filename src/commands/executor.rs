use super::{RedisCommand, RedisResponse};
use crate::storage::{MemoryStorage, Storage, StorageList};

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
            RedisCommand::RPUSH(key, value) => {
                let length = self.storage.rpush(key, value);
                RedisResponse::Integer(length as i64)
            }
            RedisCommand::LRANGE(key, start , end ) => {
                let list = self.storage.lrange(&key, start, end);
                match list {
                    Some(items) => {
                        if items.is_empty() {
                            RedisResponse::Array(vec![])
                        } else {
                            RedisResponse::Array(items.into_iter().map(|item| RedisResponse::SimpleString(item)).collect())
                        }
                    }
                    None => RedisResponse::Array(vec![]),
                }
            }
        }
    }
}

impl Default for RedisCommandExecutor {
    fn default() -> Self {
        Self::new()
    }
}