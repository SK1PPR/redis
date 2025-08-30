use super::{MemoryStorage, Storage, StorageGeo};
use crate::storage::zset_member::ZSetMember;

impl StorageGeo for MemoryStorage {
    fn geoadd(
        &mut self,
        key: String,
        longitude: f64,
        latitude: f64,
        member: String,
    ) -> Result<usize, String> {
        log::debug!(
            "Adding member '{}' with coordinates ({}, {}) to geo set '{}'",
            member,
            longitude,
            latitude,
            key
        );

        if !validate_latitude(latitude) || !validate_longitude(longitude) {
            return Err(format!(
                "invalid longitude,latitude pair {},{}",
                longitude, latitude
            ));
        }

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
                    return Ok(1);
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    // Check if member already exists
                    if zset.iter().any(|m| m.member == geo_member.member) {
                        // ZSetMember exists, update coordinates
                        zset.retain(|m| m.member != geo_member.member); // Remove old entry
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0)); // Insert updated entry
                        return Ok(0);
                    } else {
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0));
                        return Ok(1);
                    }
                }
                Ok(0)
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(ZSetMember::geo_member(longitude, latitude, member, 0.0));
                let new_unit = crate::storage::Unit::new_zset(new_set, None);
                self.storage.insert(key, new_unit);
                Ok(1)
            }
        }
    }
}

fn validate_longitude(longitude: f64) -> bool {
    longitude >= -180.0 && longitude <= 180.0
}

fn validate_latitude(latitude: f64) -> bool {
    latitude >= -85.05112878 && latitude <= 85.05112878
}
