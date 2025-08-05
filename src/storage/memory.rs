use super::{Storage, StorageList};
use std::collections::HashMap;

#[derive(Debug)]
pub struct MemoryStorage {
    data: HashMap<String, String>,
    expiry: HashMap<String, u128>,
    list: HashMap<String, Vec<String>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
            list: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.data.keys()
    }
}

impl Storage for MemoryStorage {
    fn get(&mut self, key: &str) -> Option<String> {
        log::debug!("Getting value for key '{}'", key);
        if let Some(expiry_time) = self.expiry.get(key) {
            if *expiry_time < std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() {
                log::debug!("Key '{}' has expired", key);
                self.delete(key);
                return None; // Key has expired
            }
        }
        self.data.get(key).cloned()
    }

    fn set(&mut self, key: String, value: String) {
        log::debug!("Setting key '{}' to '{}'", key, value);
        self.data.insert(key, value);
    }

    fn set_with_expiry(&mut self, key: String, value: String, expiry: u128) {
        log::debug!("Setting expiry for key '{}' to {}", key, expiry);
        self.data.insert(key.clone(), value);
        self.expiry.insert(key, expiry + std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
    }

    fn delete(&mut self, key: &str) -> bool {
        log::debug!("Deleting key '{}'", key);
        self.data.remove(key).is_some()
    }

    fn exists(&self, key: &str) -> bool {
        self.data.contains_key(key)
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
        let list = self.list.entry(key).or_insert_with(Vec::new);
        for item in value {
            list.push(item);
        }
        list.len()
    }

    fn lrange(&self, key: &str, start: i64, end: i64) -> Option<Vec<String>> {
        log::debug!("LRANGE on key '{}', start {}, end {}", key, start, end);

        let mut start_idx = start;
        let mut end_idx = end;

        if start_idx < 0 {
            start_idx = self.list.get(key)?.len() as i64 + start_idx; // Handle negative start index
        }

        if end_idx < 0 {
            end_idx = self.list.get(key)?.len() as i64 + end_idx; // Handle negative end index
        }

        let start = start_idx.max(0) as usize; // Ensure start is not negative
        let end = end_idx.max(0) as usize; // Ensure end is not negative

        if !self.list.contains_key(key) {
            log::debug!("Key '{}' does not exist in list", key);
        if start > end {
            log::debug!("Invalid range: start {} is greater than end {}", start, end);
            return Some(vec![]);
        }
            log::debug!("Invalid range: start {} is greater than end {}", start, end);
            return None;
        }

        if start >= self.list.get(key)?.len() {
            log::debug!("Start index {} is out of bounds for key '{}'", start, key);
            return Some(vec![]); // Return empty list if start is out of bounds
        }

        let end = if end >= self.list.get(key)?.len() {
            self.list.get(key)?.len() - 1 // Adjust end to the last index if it exceeds the list length
        } else {
            end
        };

        self.list.get(key).map(|items| {
            items.iter().skip(start).take(end - start + 1).cloned().collect()
        })
    }

    fn lpush(&mut self, key: String, value: Vec<String>) -> usize {
        log::debug!("LPUSH on key '{}', value '{}'", key, value.join(", "));
        let list = self.list.entry(key).or_insert_with(Vec::new);
        for item in value {
            list.insert(0, item);
        }
        list.len()
    }

    fn llen(&self, key: &str) -> usize {
        log::debug!("LLEN on key '{}'", key);
        self.list.get(key).map_or(0, |list| list.len())
    }

    fn lpop(&mut self, key: &str, count: usize) -> Option<Vec<String>> {
        log::debug!("LPOP on key '{}', count {}", key, count);
        if let Some(list) = self.list.get_mut(key) {
            if list.is_empty() {
                log::debug!("List for key '{}' is empty", key);
                return Some(vec![]);
            }
            let items_to_pop = list.drain(0..count.min(list.len())).collect();
            if list.is_empty() {
                self.list.remove(key); // Remove the key if the list is empty after popping
            }
            Some(items_to_pop)
        } else {
            log::debug!("Key '{}' does not exist in list", key);
            None
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}