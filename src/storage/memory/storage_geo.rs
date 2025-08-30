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
        let score = interleave_bits(
            truncate_to_int(normalize_longitude(latitude)),
            truncate_to_int(normalize_latitude(longitude)),
        ) as f64;
        let geo_member = ZSetMember::geo_member(longitude, latitude, member.clone(), score);
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
                        score,
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
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, score)); // Insert updated entry
                        return Ok(0);
                    } else {
                        zset.insert(ZSetMember::geo_member(longitude, latitude, member, score));
                        return Ok(1);
                    }
                }
                Ok(0)
            }
            None => {
                let mut new_set = std::collections::BTreeSet::new();
                new_set.insert(ZSetMember::geo_member(longitude, latitude, member, score));
                let new_unit = crate::storage::Unit::new_zset(new_set, None);
                self.storage.insert(key, new_unit);
                Ok(1)
            }
        }
    }
}

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

fn validate_longitude(longitude: f64) -> bool {
    longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE
}

fn validate_latitude(latitude: f64) -> bool {
    latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE
}

fn normalize_longitude(longitude: f64) -> f64 {
    ((1 << 26) as f64) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE
}

fn normalize_latitude(latitude: f64) -> f64 {
    ((1 << 26) as f64) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
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
    spread_bits(x) | (spread_bits(y) << 1)
}

#[test]
fn test_score() {
    let longitude = 100.5252;
    let latitude = 13.7220;
    let normalized_longitude = normalize_longitude(longitude);
    let normalized_latitude = normalize_latitude(latitude);
    let lon_int = truncate_to_int(normalized_longitude);
    let lat_int = truncate_to_int(normalized_latitude);
    let score = interleave_bits(lat_int, lon_int) as f64;
    println!("Score: {}", score);
    assert_eq!(score, 3962257306574459.0);
}
