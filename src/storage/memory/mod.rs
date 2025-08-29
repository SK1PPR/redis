use mio::Token;
use std::collections::HashMap;
use std::time::Instant;

use crate::commands::response::RedisResponse;
use crate::server::event_loop_handle::EventLoopHandle;
use crate::storage::{Storage, StorageList, StorageStream, StorageZSet, Unit};

mod storage;
mod storage_list;
mod storage_stream;
mod storage_zset;

#[derive(Debug, Clone)]
pub struct BlockedClient {
    token: Token,
    timeout: Option<Instant>,
    left_blocked: bool,
}

#[derive(Debug)]
pub struct MemoryStorage {
    storage: HashMap<String, Unit>,
    handle: EventLoopHandle,
    blocked_clients: HashMap<String, Vec<BlockedClient>>,
}

impl MemoryStorage {
    pub fn new(handle: EventLoopHandle) -> Self {
        Self {
            storage: HashMap::new(),
            handle,
            blocked_clients: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn clear(&mut self) {
        self.storage.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.storage.keys()
    }

    // Helper method to unblock clients waiting on a specific key
    fn unblock_clients_for_key(&mut self, key: &str) {
        if let Some(blocked_clients) = self.blocked_clients.remove(key) {
            log::debug!(
                "Unblocking {} clients waiting for key '{}'",
                blocked_clients.len(),
                key
            );

            for blocked_client in blocked_clients.clone() {
                // Make sure the client is still blocked
                if blocked_client.timeout.is_some()
                    && blocked_client.timeout.unwrap() < Instant::now()
                {
                    log::debug!(
                        "Client with token {:?} has timed out while waiting for key '{}'",
                        blocked_client.token,
                        key
                    );
                    continue; // Skip timed-out clients
                }

                // Try to pop an element for this client
                let response = if blocked_client.left_blocked {
                    // BLPOP - pop from left
                    if let Some(mut popped) = self.lpop(key, 1) {
                        if !popped.is_empty() {
                            RedisResponse::Array(vec![
                                RedisResponse::BulkString(Some(key.to_string())),
                                RedisResponse::BulkString(Some(popped.remove(0))),
                            ])
                        } else {
                            continue; // No element available, keep client blocked
                        }
                    } else {
                        continue; // Key doesn't exist or no elements
                    }
                } else {
                    // BRPOP - pop from right
                    if let Some(list) = self
                        .storage
                        .get_mut(key)
                        .and_then(|unit| unit.implementation.as_list_mut())
                    {
                        if !list.is_empty() {
                            let item = list.remove(list.len() - 1);
                            if list.is_empty() {
                                self.storage.remove(key);
                            }
                            RedisResponse::Array(vec![
                                RedisResponse::BulkString(Some(key.to_string())),
                                RedisResponse::BulkString(Some(item)),
                            ])
                        } else {
                            continue; // No elements available
                        }
                    } else {
                        continue; // Key doesn't exist
                    }
                };

                // Unblock the client with the response
                self.handle.unblock_client(blocked_client.token, response);

                // If we successfully unblocked a client, we might have consumed the only element
                // Check if there are more elements for remaining clients
                if let Some(list) = self
                    .storage
                    .get(key)
                    .and_then(|unit| unit.implementation.as_list())
                {
                    if list.is_empty() {
                        // No more elements, re-block remaining clients
                        self.reblock_remaining_clients(key, blocked_clients, blocked_client.token);
                        break;
                    }
                } else {
                    // Key no longer exists, re-block remaining clients
                    self.reblock_remaining_clients(key, blocked_clients, blocked_client.token);
                    break;
                }
            }
        }
    }

    // Helper to re-block clients that couldn't be satisfied
    fn reblock_remaining_clients(
        &mut self,
        key: &str,
        mut all_clients: Vec<BlockedClient>,
        satisfied_token: Token,
    ) {
        // Remove the client we just satisfied and re-block the rest
        all_clients.retain(|client| client.token.0 != satisfied_token.0);

        if !all_clients.is_empty() {
            self.blocked_clients.insert(key.to_string(), all_clients);
            log::debug!(
                "Re-blocked {} clients for key '{}'",
                self.blocked_clients.get(key).map_or(0, |v| v.len()),
                key
            );
        }
    }

    pub fn get_type(&self, key: &str) -> String {
        if let Some(unit) = self.storage.get(key) {
            if unit.is_expired() {
                log::debug!("Key '{}' has expired", key);
                return "none".to_string();
            }
            if unit.implementation.is_string() {
                "string".to_string()
            } else if unit.implementation.is_list() {
                "list".to_string()
            } else if unit.implementation.is_zset() {
                "zset".to_string()
            } else if unit.implementation.is_stream() {
                "stream".to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "none".to_string()
        }
    }
}
