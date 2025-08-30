use super::{BlockedClient, MemoryStorage, Storage, StorageList, Unit};
use mio::Token;
use std::time::{Duration, Instant};

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
        self.unblock_clients_for_key(&key, true);

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
        self.unblock_clients_for_key(&key, true);

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

    fn blpop(&mut self, keys: Vec<String>, token: Token, timeout: u64) -> Option<Vec<String>> {
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

        let blocked_client = BlockedClient::new_list(
            token,
            if timeout != 0 {
                Some(Instant::now() + Duration::from_millis(timeout as u64))
            } else {
                None
            },
            true,
        );

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

    fn brpop(&mut self, keys: Vec<String>, token: Token, timeout: u64) -> Option<Vec<String>> {
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

        let blocked_client = BlockedClient::new_list(
            token,
            if timeout != 0 {
                Some(Instant::now() + Duration::from_millis(timeout as u64))
            } else {
                None
            },
            false,
        );

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
