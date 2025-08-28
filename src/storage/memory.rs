use super::unit::Unit;
use super::{Storage, StorageList};
use crate::server::event_loop_handle::EventLoopHandle;
use crate::RedisResponse;
use mio::Token;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct BlockedClient {
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
}

impl Storage for MemoryStorage {
    fn get(&mut self, key: &str) -> Option<String> {
        log::debug!("Getting value for key '{}'", key);
        let value = self.storage.get(key).cloned()?;
        if value.is_expired() || !value.implementation.is_string() {
            log::debug!("Key '{}' has expired", key);
            self.delete(key);
            return None; // Key has expired
        }
        return value.implementation.as_string().cloned();
    }

    fn set(&mut self, key: String, value: String) {
        log::debug!("Setting key '{}' to '{}'", key, value);
        let unit = Unit::new_string(value, None);
        self.storage.insert(key, unit);
    }

    fn set_with_expiry(&mut self, key: String, value: String, expiry: u128) {
        log::debug!("Setting expiry for key '{}' to {}", key, expiry);
        let unit = Unit::new_string(
            value,
            Some(
                expiry
                    + std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
            ),
        );
        self.storage.insert(key.clone(), unit);
    }

    fn delete(&mut self, key: &str) -> bool {
        log::debug!("Deleting key '{}'", key);
        self.storage.remove(key).is_some()
    }

    fn exists(&self, key: &str) -> bool {
        self.storage.contains_key(key)
    }

    fn delete_multiple(&mut self, keys: Vec<String>) -> usize {
        let mut deleted = 0;
        for key in keys {
            if self.delete(&key) {
                deleted += 1;
            }
        }
        deleted
    }

    fn exists_multiple(&self, keys: &[String]) -> usize {
        keys.iter().filter(|key| self.exists(key)).count()
    }
}

impl StorageList for MemoryStorage {
    fn rpush(&mut self, key: String, value: Vec<String>) -> usize {
        log::debug!("RPUSH on key '{}', value '{}'", key, value.join(", "));

        let list_length = if self.exists(&key) {
            if !self.storage.get(&key).unwrap().implementation.is_list() {
                log::debug!("Key '{}' exists but is not a list, converting to list", key);
                let existing_value = self.storage.remove(&key).unwrap();
                let new_list = vec![existing_value.implementation.as_string().unwrap().clone()];
                let unit = Unit::new_list(new_list, None);
                self.storage.insert(key.clone(), unit);
            } else {
                log::debug!("Key '{}' already exists, appending to list", key);
            }
            self.storage
                .get_mut(&key)
                .unwrap()
                .implementation
                .extend_list(value);
            self.storage
                .get(&key)
                .unwrap()
                .implementation
                .get_list_length()
        } else {
            log::debug!("Key '{}' does not exist, creating new list", key);
            let unit = Unit::new_list(value.clone(), None);
            self.storage.insert(key.clone(), unit);
            value.len()
        };

        // Unblock any clients waiting for this key
        self.unblock_clients_for_key(&key);

        list_length
    }

    fn lrange(&self, key: &str, start: i64, end: i64) -> Option<Vec<String>> {
        log::debug!("LRANGE on key '{}', start {}, end {}", key, start, end);

        let mut start_idx = start;
        let mut end_idx = end;

        let list = self.storage.get(key)?.implementation.as_list()?;

        if start_idx < 0 {
            start_idx = list.len() as i64 + start_idx; // Handle negative start index
        }

        if end_idx < 0 {
            end_idx = list.len() as i64 + end_idx; // Handle negative end index
        }

        let start = start_idx.max(0) as usize; // Ensure start is not negative
        let end = end_idx.max(0) as usize; // Ensure end is not negative

        if !self.exists(key) {
            log::debug!("Key '{}' does not exist in list", key);
            return None;
        }
        if start > end {
            log::debug!("Invalid range: start {} is greater than end {}", start, end);
            return Some(vec![]);
        }

        if start >= list.len() {
            log::debug!("Start index {} is out of bounds for key '{}'", start, key);
            return Some(vec![]); // Return empty list if start is out of bounds
        }

        let end = if end >= list.len() {
            list.len() - 1 // Adjust end to the last index if it exceeds the list length
        } else {
            end
        };

        Some(list[start..=end].to_vec())
    }

    fn lpush(&mut self, key: String, value: Vec<String>) -> usize {
        log::debug!("LPUSH on key '{}', value '{}'", key, value.join(", "));

        let list_length = if self.exists(&key) {
            if self.storage.get(&key).unwrap().implementation.is_list() {
                log::debug!("Key '{}' exists and is a list, prepending to list", key);
            } else {
                log::debug!("Key '{}' exists but is not a list, converting to list", key);
                let existing_value = self.storage.remove(&key).unwrap();
                let new_list = vec![existing_value.implementation.as_string().unwrap().clone()];
                let unit = Unit::new_list(new_list, None);
                self.storage.insert(key.clone(), unit);
            }

            let list = self
                .storage
                .get_mut(&key)
                .unwrap()
                .implementation
                .as_list_mut()
                .unwrap();

            // Insert at the beginning (prepend)
            for (i, item) in value.iter().rev().enumerate() {
                list.insert(i, item.clone());
            }
            list.len()
        } else {
            log::debug!("Key '{}' does not exist, creating new list", key);
            let unit = Unit::new_list(value.clone(), None);
            self.storage.insert(key.clone(), unit);
            value.len()
        };

        // Unblock any clients waiting for this key
        self.unblock_clients_for_key(&key);

        list_length
    }

    fn llen(&self, key: &str) -> usize {
        log::debug!("LLEN on key '{}'", key);
        self.storage
            .get(key)
            .and_then(|unit| unit.implementation.as_list())
            .map_or(0, |list| list.len())
    }

    fn lpop(&mut self, key: &str, count: usize) -> Option<Vec<String>> {
        log::debug!("LPOP on key '{}', count {}", key, count);
        if let Some(list) = self
            .storage
            .get_mut(key)
            .and_then(|unit| unit.implementation.as_list_mut())
        {
            if list.is_empty() {
                log::debug!("List for key '{}' is empty", key);
                return Some(vec![]);
            }
            let items_to_pop = list.drain(0..count.min(list.len())).collect();
            if list.is_empty() {
                self.storage.remove(key); // Remove the key if the list is empty after popping
            }
            Some(items_to_pop)
        } else {
            log::debug!("Key '{}' does not exist in list", key);
            None
        }
    }

    fn blpop(&mut self, keys: Vec<String>, token: Token, timeout: u32) -> Option<Vec<String>> {
        log::debug!("BLPOP on keys '{:?}', timeout {}", keys, timeout);

        // First, try to get an element immediately from any of the keys
        for key in &keys {
            if let Some(list) = self
                .storage
                .get_mut(key)
                .and_then(|unit| unit.implementation.as_list_mut())
            {
                if !list.is_empty() {
                    let item = list.remove(0);
                    if list.is_empty() {
                        self.storage.remove(key); // Remove the key if the list is empty after popping
                    }
                    return Some(vec![key.clone(), item]);
                }
            }
        }

        // If none of the lists have elements, block the client
        let blocked_client = BlockedClient {
            token,
            timeout: if timeout != 0 {
                Some(Instant::now() + Duration::from_secs(timeout as u64))
            } else {
                None
            },
            left_blocked: true,
        };

        for key in keys {
            self.blocked_clients
                .entry(key)
                .or_insert_with(Vec::new)
                .push(blocked_client.clone());
        }

        // Tell the event loop to block this client
        self.handle.block_client(token, timeout);

        None
    }

    fn brpop(&mut self, keys: Vec<String>, token: Token, timeout: u32) -> Option<Vec<String>> {
        log::debug!("BRPOP on keys '{:?}', timeout {}", keys, timeout);

        // First, try to get an element immediately from any of the keys
        for key in &keys {
            if let Some(list) = self
                .storage
                .get_mut(key)
                .and_then(|unit| unit.implementation.as_list_mut())
            {
                if !list.is_empty() {
                    let item = list.remove(list.len() - 1);
                    if list.is_empty() {
                        self.storage.remove(key); // Remove the key if the list is empty after popping
                    }
                    return Some(vec![key.clone(), item]);
                }
            }
        }

        // If none of the lists have elements, block the client
        let blocked_client = BlockedClient {
            token,
            timeout: if timeout != 0 {
                Some(Instant::now() + Duration::from_secs(timeout as u64))
            } else {
                None
            },
            left_blocked: false,
        };

        for key in keys {
            self.blocked_clients
                .entry(key)
                .or_insert_with(Vec::new)
                .push(blocked_client.clone());
        }

        // Tell the event loop to block this client
        self.handle.block_client(token, timeout);

        None
    }
}
