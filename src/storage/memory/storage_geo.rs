use super::{MemoryStorage, Storage, StorageGeo};
use crate::storage::zset_member::ZSetMember;

impl StorageGeo for MemoryStorage {
    fn geoadd(&mut self, key: String, longitude: f64, latitude: f64, member: String) -> usize {
        log::debug!(
            "Adding member '{}' with coordinates ({}, {}) to geo set '{}'",
            member,
            longitude,
            latitude,
            key
        );
        let geo_member = ZSetMember::geo_member(longitude, latitude, member.clone(), 0.0);
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() || !u.implementation.is_zset_geo()
                {
                    log::debug!("Key '{}' has expired or is not a geo set", key);
                    self.delete(&key);
                    let mut new_set = std::collections::BTreeSet::new();
                    new_set.insert(ZSetMember::geo_member(
                        longitude,
                        latitude,
                        member.clone(),
                        0.0,
                    ));
                    let new_unit = crate::storage::Unit::new_zset(new_set, None);
                    self.storage.insert(key, new_unit);
                    return 1;
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    // Check if member already exists
                    if zset.iter().any(|m| m.member == geo_member.member) {
                        // ZSetMember exists, update coordinates
                        zset.retain(|m| m.member != geo_member.member); // Remove old entry
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0)); // Insert updated entry
                        return 0;
                    } else {
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0));
                        return 1;
                    }
                }
                0
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0));
                let new_unit = crate::storage::Unit::new_zset(new_set, None);
                self.storage.insert(key, new_unit);
                1
            }
        }
    }
}
