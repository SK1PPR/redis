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
                    // Check if member already exists
                    if zset.iter().any(|m| m.member == member) {
                        // Member exists, update score
                        zset.retain(|m| m.member != member); // Remove old entry
                        zset.insert(Member { score, member }); // Insert updated entry
                        return 0;
                    } else {
                        zset.insert(Member { score, member });
                        return 1;
                    }
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
        log::debug!(
            "Getting rank of member '{}' in sorted set '{}'",
            member,
            key
        );
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

    fn zrange(&self, key: &str, start: i64, end: i64) -> Option<Vec<String>> {
        log::debug!(
            "Getting range from sorted set '{}', start {}, end {}",
            key,
            start,
            end
        );
        let unit = self.storage.get(key)?;
        if unit.is_expired() || !unit.implementation.is_zset() {
            log::debug!("Key '{}' has expired or is not a sorted set", key);
            return None;
        }
        let zset = unit.implementation.as_zset()?;
        let len = zset.len() as i64;

        let mut start_idx = start;
        let mut end_idx = end;

        if start_idx < 0 {
            start_idx = len + start_idx; // Handle negative start index
        }

        if end_idx < 0 {
            end_idx = len + end_idx; // Handle negative end index
        }

        let start = start_idx.max(0) as usize; // Ensure start is not negative
        let end = end_idx.max(0) as usize; // Ensure end is not negative

        if start > end || start >= len as usize {
            return Some(vec![]); // Invalid range
        }

        let end = end.min(len as usize - 1); // Ensure end does not exceed length

        Some(
            zset.iter()
                .skip(start)
                .take(end - start + 1)
                .map(|m| m.member.clone())
                .collect(),
        )
    }

    fn zcard(&self, key: &str) -> usize {
        log::debug!("Getting cardinality of sorted set '{}'", key);
        let unit = self.storage.get(key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a sorted set", key);
                    return 0;
                }
                if let Some(zset) = u.implementation.as_zset() {
                    return zset.len();
                }
                0
            }
            None => 0,
        }
    }

    fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        log::debug!(
            "Getting score of member '{}' in sorted set '{}'",
            member,
            key
        );
        let unit = self.storage.get(key)?;
        if unit.is_expired() || !unit.implementation.is_zset() {
            log::debug!("Key '{}' has expired or is not a sorted set", key);
            return None;
        }
        let zset = unit.implementation.as_zset()?;
        for m in zset.iter() {
            if m.member == member {
                return Some(m.score);
            }
        }
        None
    }

    fn zrem(&mut self, key: &str, member: &str) -> bool {
        log::debug!("Removing member '{}' from sorted set '{}'", member, key);
        let unit = self.storage.get_mut(key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a sorted set", key);
                    self.delete(key);
                    return false;
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    let initial_len = zset.len();
                    zset.retain(|m| m.member != member);
                    return zset.len() < initial_len;
                }
                false
            }
            None => false,
        }
    }
}
