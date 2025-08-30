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

        if !GeoUtils::validate_location(longitude, latitude) {
            return Err(format!(
                "invalid longitude,latitude pair {},{}",
                longitude, latitude
            ));
        }
        let score = GeoUtils::calculate_score(longitude, latitude);
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a geo set", key);
                    self.delete(&key);
                    let mut new_set = std::collections::BTreeSet::new();
                    new_set.insert(ZSetMember { score, member });
                    let new_unit = crate::storage::Unit::new_zset(new_set, None);
                    self.storage.insert(key, new_unit);
                    return Ok(1);
                }
                if let Some(zset) = u.implementation.as_zset_mut() {
                    // Check if member already exists
                    if zset.iter().any(|m| m.member == member) {
                        // ZSetMember exists, update coordinates
                        zset.retain(|m| m.member != member); // Remove old entry
                        zset.insert(ZSetMember { score, member }); // Insert updated entry
                        return Ok(0);
                    } else {
                        zset.insert(ZSetMember { score, member });
                        return Ok(1);
                    }
                }
                Ok(0)
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(ZSetMember { score, member });
                let new_unit = crate::storage::Unit::new_zset(new_set, None);
                self.storage.insert(key, new_unit);
                Ok(1)
            }
        }
    }
}

struct GeoUtils;

impl GeoUtils {
    const MIN_LATITUDE: f64 = -85.05112878;
    const MAX_LATITUDE: f64 = 85.05112878;
    const MIN_LONGITUDE: f64 = -180.0;
    const MAX_LONGITUDE: f64 = 180.0;

    const LATITUDE_RANGE: f64 = Self::MAX_LATITUDE - Self::MIN_LATITUDE;
    const LONGITUDE_RANGE: f64 = Self::MAX_LONGITUDE - Self::MIN_LONGITUDE;

    fn validate_longitude(longitude: f64) -> bool {
        longitude >= Self::MIN_LONGITUDE && longitude <= Self::MAX_LONGITUDE
    }

    fn validate_latitude(latitude: f64) -> bool {
        latitude >= Self::MIN_LATITUDE && latitude <= Self::MAX_LATITUDE
    }

    fn normalize_longitude(longitude: f64) -> f64 {
        ((1 << 26) as f64) * (longitude - Self::MIN_LONGITUDE) / Self::LONGITUDE_RANGE
    }

    fn normalize_latitude(latitude: f64) -> f64 {
        ((1 << 26) as f64) * (latitude - Self::MIN_LATITUDE) / Self::LATITUDE_RANGE
    }

    fn truncate_to_int(value: f64) -> u32 {
        value as u32
    }

    fn spread_bits(value: u32) -> u64 {
        let mut x = value as u64;
        x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
        x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
        x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
        x = (x | (x << 2)) & 0x3333333333333333;
        x = (x | (x << 1)) & 0x5555555555555555;
        x
    }

    fn interleave_bits(x: u32, y: u32) -> u64 {
        Self::spread_bits(x) | (Self::spread_bits(y) << 1)
    }

    pub fn calculate_score(longitude: f64, latitude: f64) -> f64 {
        let norm_long = Self::normalize_longitude(longitude);
        let norm_lat = Self::normalize_latitude(latitude);
        let int_long = Self::truncate_to_int(norm_long);
        let int_lat = Self::truncate_to_int(norm_lat);
        Self::interleave_bits(int_lat, int_long) as f64
    }

    pub fn validate_location(longitude: f64, latitude: f64) -> bool {
        Self::validate_longitude(longitude) && Self::validate_latitude(latitude)
    }
}
