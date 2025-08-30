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

    fn geopos(&self, key: &str, members: Vec<String>) -> Vec<Option<(f64, f64)>> {
        log::debug!(
            "Retrieving positions of members '{:?}' from geo set '{}'",
            members,
            key
        );

        let unit = self.storage.get(key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a geo set", key);
                    return vec![None; members.len()];
                }

                if let Some(zset) = u.implementation.as_zset() {
                    // Process each member and collect results
                    members
                        .iter()
                        .map(|member| {
                            zset.iter()
                                .find(|m| m.member == *member)
                                .map(|zset_member| GeoUtils::decode_score(zset_member.score))
                        })
                        .collect()
                } else {
                    vec![None; members.len()]
                }
            }
            None => vec![None; members.len()],
        }
    }

    fn geodist(&self, key: &str, member1: &str, member2: &str) -> Option<f64> {
        log::debug!(
            "Calculating distance between members '{}' and '{}' in geo set '{}'",
            member1,
            member2,
            key
        );

        let unit = self.storage.get(key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_zset() {
                    log::debug!("Key '{}' has expired or is not a geo set", key);
                    return None;
                }

                if let Some(zset) = u.implementation.as_zset() {
                    let pos1 = zset.iter().find(|m| m.member == member1);
                    let pos2 = zset.iter().find(|m| m.member == member2);

                    match (pos1, pos2) {
                        (Some(m1), Some(m2)) => {
                            let distance = GeoUtils::calculate_distance(m1.score, m2.score);
                            Some(distance)
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            None => None,
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

    fn compact_bits(value: u64) -> u32 {
        let mut x = value & 0x5555555555555555;
        x = (x | (x >> 1)) & 0x3333333333333333;
        x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F;
        x = (x | (x >> 4)) & 0x00FF00FF00FF00FF;
        x = (x | (x >> 8)) & 0x0000FFFF0000FFFF;
        x = (x | (x >> 16)) & 0x00000000FFFFFFFF;
        x as u32
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

    pub fn decode_score(score: f64) -> (f64, f64) {
        let score = score as u64;
        let lat = Self::compact_bits(score);
        let lon = Self::compact_bits(score >> 1);

        // Use higher precision for intermediate calculations
        let lat_f = lat as f64;
        let lon_f = lon as f64;
        let lat_plus_one = (lat + 1) as f64;
        let lon_plus_one = (lon + 1) as f64;
        let shift_26 = (1 << 26) as f64;

        // Calculate latitude and longitude ranges with higher precision
        let lat_min = Self::MIN_LATITUDE + Self::LATITUDE_RANGE * lat_f / shift_26;
        let lat_max = Self::MIN_LATITUDE + Self::LATITUDE_RANGE * lat_plus_one / shift_26;
        
        let lon_min = Self::MIN_LONGITUDE + Self::LONGITUDE_RANGE * lon_f / shift_26;
        let lon_max = Self::MIN_LONGITUDE + Self::LONGITUDE_RANGE * lon_plus_one / shift_26;

        // Return the midpoints as f64 at the end
        (
            (lon_min + lon_max) / 2.0,
            (lat_min + lat_max) / 2.0,
        )
    }

    pub fn calculate_distance(from: f64, to: f64) -> f64 {
        let (lon1, lat1) = Self::decode_score(from);
        let (lon2, lat2) = Self::decode_score(to);
        let lat1_rad = lat1.to_radians();
        let lat2_rad = lat2.to_radians();
        let d_lat = lat2_rad - lat1_rad;
        let d_lon = (lon2 - lon1).to_radians();
        const R: f64 = 6372797.560856; // Radius of the Earth in meters
        let a = (d_lat / 2.0).sin().powi(2)
            + lat1_rad.cos() * lat2_rad.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();
        R * c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_score() {
        let test_cases = [
            // City, Latitude, Longitude, Expected Score
            ("Bangkok", 13.7220, 100.5252, 3962257306574459.0),
            ("Beijing", 39.9075, 116.3972, 4069885364908765.0),
            ("Berlin", 52.5244, 13.4105, 3673983964876493.0),
            ("Copenhagen", 55.6759, 12.5655, 3685973395504349.0),
            ("New Delhi", 28.6667, 77.2167, 3631527070936756.0),
            ("Kathmandu", 27.7017, 85.3206, 3639507404773204.0),
            ("London", 51.5074, -0.1278, 2163557714755072.0),
            ("New York", 40.7128, -74.0060, 1791873974549446.0),
            ("Paris", 48.8534, 2.3488, 3663832752681684.0),
            ("Sydney", -33.8688, 151.2093, 3252046221964352.0),
            ("Tokyo", 35.6895, 139.6917, 4171231230197045.0),
            ("Vienna", 48.2064, 16.3707, 3673109836391743.0),
        ];

        for (city, latitude, longitude, expected_score) in test_cases {
            let score = GeoUtils::calculate_score(longitude, latitude);
            assert_eq!(score, expected_score, "Score for {} should match", city);

            // Verify that decode_score returns coordinates close to the original
            let (decoded_longitude, decoded_latitude) = GeoUtils::decode_score(score);

            // We allow a small margin of error due to the grid-based nature of the encoding
            const EPSILON: f64 = 0.01;
            assert!(
                (decoded_longitude - longitude).abs() < EPSILON,
                "Decoded longitude for {} should be close to original: expected {}, got {}",
                city,
                longitude,
                decoded_longitude
            );
            assert!(
                (decoded_latitude - latitude).abs() < EPSILON,
                "Decoded latitude for {} should be close to original: expected {}, got {}",
                city,
                latitude,
                decoded_latitude
            );
        }
    }

    #[test]
    fn test_haversine() {
        let score1 = GeoUtils::calculate_score(-86.67, 36.12);
        let score2 = GeoUtils::calculate_score(-118.4, 33.94);
        let distance = GeoUtils::calculate_distance(score1, score2);
        assert!(
            (distance - 2886444.062365684).abs() < 1e-6,
            "Distance in meters should match"
        );
    }
}
