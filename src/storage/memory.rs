use super::Storage;
use std::collections::HashMap;

#[derive(Debug)]
pub struct MemoryStorage {
    data: HashMap<String, String>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
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
    fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }
    
    fn set(&mut self, key: String, value: String) {
        log::debug!("Setting key '{}' to '{}'", key, value);
        self.data.insert(key, value);
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
        keys.iter()
            .filter(|key| self.exists(key))
            .count()
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}