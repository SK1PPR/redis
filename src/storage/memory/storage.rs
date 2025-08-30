use super::{MemoryStorage, Storage, Unit};
use regex;

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

    fn incr(&mut self, key: String) -> Option<i64> {
        log::debug!("Incrementing value for key '{}'", key);
        if let Some(unit) = self.storage.get_mut(&key) {
            if unit.implementation.is_string() {
                if let Some(current_value) = unit.implementation.as_string() {
                    if unit.is_expired() {
                        log::debug!("Key '{}' has expired", key);
                        self.delete(&key);
                        // Initialize to 1 if expired
                        let unit = Unit::new_string("1".to_string(), None);
                        self.storage.insert(key, unit);
                        return Some(1);
                    }

                    match current_value.parse::<i64>() {
                        Ok(num) => {
                            if let Some(new_value) = num.checked_add(1) {
                                unit.implementation =
                                    Unit::new_string(new_value.to_string(), unit.expiry)
                                        .implementation;
                                return Some(new_value);
                            } else {
                                return None; // Integer overflow occurred
                            }
                        }
                        Err(_) => return None, // Value is not an integer
                    }
                }
                return None;
            } else {
                return None; // Value is not a string
            }
        } else {
            // Key does not exist, initialize to 1
            let unit = Unit::new_string("1".to_string(), None);
            self.storage.insert(key, unit);
            return Some(1);
        }
    }

    fn config_get(&self, parameter: &str) -> Option<String> {
        match parameter.to_lowercase().as_str() {
            "dir" => self.dir.clone(),
            "dbfilename" => self.dbfilename.clone(),
            _ => None,
        }
    }

    fn get_keys(&self, pattern: &str) -> Vec<String> {
        let regex_pattern = pattern.replace("*", ".*").replace("?", ".");
        let regex = match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(r) => r,
            Err(e) => {
                log::error!("Invalid pattern '{}': {}", pattern, e);
                return vec![];
            }
        };

        self.storage
            .keys()
            .filter(|key| regex.is_match(key))
            .cloned()
            .collect()
    }
}
