use super::{Storage, StorageList};
use std::collections::HashMap;
use super::unit::Unit;

#[derive(Debug)]
pub struct MemoryStorage {
    storage: HashMap<String, Unit>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
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
        let unit = Unit::new_string(value, Some(expiry + std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()));
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

        if self.exists(&key) {
            if !self.storage.get(&key).unwrap().implementation.is_list() {
                log::debug!("Key '{}' exists but is not a list, converting to list", key);
                let existing_value = self.storage.remove(&key).unwrap();
                let new_list = vec![existing_value.implementation.as_string().unwrap().clone()];
                let unit = Unit::new_list(new_list, None);
                self.storage.insert(key.clone(), unit);
            } else {
                log::debug!("Key '{}' already exists, appending to list", key);
            }
            self.storage.get_mut(&key).unwrap().implementation.extend_list(value);
            return self.storage.get(&key).unwrap().implementation.get_list_length();
        } else {
            log::debug!("Key '{}' does not exist, creating new list", key);
            let unit = Unit::new_list(value.clone(), None);
            self.storage.insert(key.clone(), unit);
            return value.len();
        }
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
        if self.exists(&key) {
            if self.storage.get(&key).unwrap().implementation.is_list() {
                log::debug!("Key '{}' exists and is a list, prepending to list", key);
            } else {
                log::debug!("Key '{}' exists but is not a list, converting to list", key);
                let existing_value = self.storage.remove(&key).unwrap();
                let new_list = vec![existing_value.implementation.as_string().unwrap().clone()];
                let unit = Unit::new_list(new_list, None);
                self.storage.insert(key.clone(), unit);
            }

            let list = self.storage.get_mut(&key).unwrap().implementation.as_list_mut().unwrap();
            list.extend(value);
            return list.len();
        }
        log::debug!("Key '{}' does not exist, creating new list", key);
        let unit = Unit::new_list(value.clone(), None);
        self.storage.insert(key.clone(), unit);
        return value.len();
    }

    fn llen(&self, key: &str) -> usize {
        log::debug!("LLEN on key '{}'", key);
        self.storage.get(key).and_then(|unit| unit.implementation.as_list()).map_or(0, |list| list.len())
    }

    fn lpop(&mut self, key: &str, count: usize) -> Option<Vec<String>> {
        log::debug!("LPOP on key '{}', count {}", key, count);
        if let Some(list) = self.storage.get_mut(key).unwrap().implementation.as_list_mut() {
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
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}