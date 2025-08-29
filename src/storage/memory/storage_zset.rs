use crate::storage::zset_member::Member;

use super::{MemoryStorage, Storage, StorageZSet, Unit};

impl StorageZSet for MemoryStorage {
    fn zadd(&mut self, key: String, score: f64, member: String) -> usize {
        log::debug!(
            "Adding member '{}' with score {} to sorted set '{}'",
            member,
            score,
            key
        );
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a sorted set", key);
                    self.delete(&key);
                    let mut new_set = std::collections::BTreeSet::new();
                    new_set.insert(Member { score, member });
                    let new_unit = Unit::new_zset(new_set, None);
                    self.storage.insert(key, new_unit);
                    return 1;
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    let initial_len = zset.len();
                    zset.replace(Member { score, member });
                    return if zset.len() > initial_len { 1 } else { 0 };
                }
                0
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(Member { score, member });
                let new_unit = Unit::new_zset(new_set, None);
                self.storage.insert(key, new_unit);
                1
            }
        }
    }

    fn zrank(&self, key: &str, member: &str) -> Option<usize> {
        log::debug!("Getting rank of member '{}' in sorted set '{}'", member, key);
        let unit = self.storage.get(key)?;
        if unit.is_expired() || !unit.implementation.is_zset() {
            log::debug!("Key '{}' has expired or is not a sorted set", key);
            return None;
        }
        let zset = unit.implementation.as_zset()?;
        for (idx, m) in zset.iter().enumerate() {
            if m.member == member {
                return Some(idx);
            }
        }
        None
    }
}
