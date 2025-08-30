#[derive(Debug, Clone)]
pub struct GeoMember {
    pub longitude: f64,
    pub latitude: f64,
    pub member: String,
}

impl PartialEq for GeoMember {
    fn eq(&self, other: &Self) -> bool {
        (self.longitude - other.longitude).abs() < f64::EPSILON
            && (self.latitude - other.latitude).abs() < f64::EPSILON
            && self.member == other.member
    }
}

impl Eq for GeoMember {}

impl PartialOrd for GeoMember {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.member.cmp(&other.member))
    }
}

impl Ord for GeoMember {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.member.cmp(&other.member)
    }
}

impl GeoMember {
    pub fn to_string(&self) -> String {
        format!("{}", self.member)
    }
}
