use crate::storage::zset_member::ZSetMember;

use super::{MemoryStorage, Storage, StorageZSet, Unit};

impl StorageZSet for MemoryStorage {
    fn zadd(&mut self, key: String, score: f64, member: String) -> usize {
        log::debug!(
            "Adding member '{}' with score {} to sorted set '{}'",
            member,
            score,
            key
        );
        let old_score = self.zset_score(&key, &member);
        let index_member = member.clone();
        let mut added = 0;
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a sorted set", key);
                    let mut new_set = std::collections::BTreeSet::new();
                    new_set.insert(ZSetMember { score, member });
                    let new_unit = Unit::new_zset(new_set, None);
                    self.remove_zset_indexes(&key);
                    self.storage.insert(key.clone(), new_unit);
                    added = 1;
                } else if let Some(zset) = u.implementation.as_zset_mut() {
                    if let Some(previous_score) = old_score {
                        zset.remove(&ZSetMember {
                            score: previous_score,
                            member: member.clone(),
                        });
                    } else {
                        added = 1;
                    }
                    zset.insert(ZSetMember { score, member });
                }
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(ZSetMember { score, member });
                let new_unit = Unit::new_zset(new_set, None);
                self.storage.insert(key.clone(), new_unit);
                added = 1;
            }
        }
        self.set_zset_score_index(&key, &index_member, score);
        added
    }

    fn zrank(&mut self, key: &str, member: &str) -> Option<usize> {
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
        if self.zset_rank_dirty.contains(key) {
            self.rebuild_zset_indexes(key);
        }
        self.zset_rank(key, member)
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
                .map(|m| m.member.clone().to_string())
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
        self.zset_score(key, member)
    }

    fn zrem(&mut self, key: &str, member: &str) -> bool {
        log::debug!("Removing member '{}' from sorted set '{}'", member, key);
        let old_score = self.zset_score(key, member);
        let unit = self.storage.get_mut(key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a sorted set", key);
                    self.delete(key);
                    return false;
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    if let Some(score) = old_score {
                        let removed = zset.remove(&ZSetMember {
                            score,
                            member: member.to_string(),
                        });
                        if removed {
                            self.remove_zset_member_index(key, member);
                        }
                        return removed;
                    }
                }
                false
            }
            None => false,
        }
    }
}
