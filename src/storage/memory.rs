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

    fn lrange(&self, key: &str, start: usize, end: usize) -> Option<Vec<String>> {
        log::debug!("LRANGE on key '{}', start {}, end {}", key, start, end);

        if !self.list.contains_key(key) {
            log::debug!("Key '{}' does not exist in list", key);
            return None;
        }

        if start > end {
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
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}