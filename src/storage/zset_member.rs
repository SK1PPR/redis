use super::geo_member::GeoMember;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub enum Member {
    Simple(String),
    Geo(GeoMember),
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Member::Simple(s1), Member::Simple(s2)) => s1 == s2,
            (Member::Geo(g1), Member::Geo(g2)) => g1 == g2,
            _ => false,
        }
    }
}

impl Eq for Member {}

impl PartialOrd for Member {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Member::Simple(s1), Member::Simple(s2)) => Some(s1.cmp(s2)),
            (Member::Geo(g1), Member::Geo(g2)) => Some(g1.cmp(g2)),
            // We should not compare different types, this is just to have a consistent ordering
            (Member::Simple(_), Member::Geo(_)) => Some(Ordering::Less), // Simple < Geo
            (Member::Geo(_), Member::Simple(_)) => Some(Ordering::Greater), // Geo > Simple
        }
    }
}

impl Ord for Member {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Member {
    pub fn to_string(&self) -> String {
        match self {
            Member::Simple(s) => s.clone(),
            Member::Geo(g) => g.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZSetMember {
    pub score: f64,
    pub member: Member,
}

impl ZSetMember {
    fn same_score(&self, other: &Self) -> bool {
        (self.score - other.score).abs() < f64::EPSILON
    }

    fn is_nan(&self) -> bool {
        self.score.is_nan()
    }

    pub fn simple_member(member: String, score: f64) -> Self {
        ZSetMember {
            score,
            member: Member::Simple(member),
        }
    }

    pub fn geo_member(longitude: f64, latitude: f64, member: String, score: f64) -> Self {
        ZSetMember {
            score,
            member: Member::Geo(GeoMember {
                longitude,
                latitude,
                member,
            }),
        }
    }

    pub fn is_geo(&self) -> bool {
        matches!(self.member, Member::Geo(_))
    }
}

impl PartialEq for ZSetMember {
    fn eq(&self, other: &Self) -> bool {
        self.same_score(other) && self.member == other.member
    }
}

impl Eq for ZSetMember {}

impl PartialOrd for ZSetMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else if self.same_score(other) {
            Some(self.member.cmp(&other.member))
        } else if self.is_nan() && other.is_nan() {
            Some(Ordering::Equal) // NaN == NaN
        } else if self.is_nan() {
            Some(Ordering::Less) // NaN < everything else
        } else if other.is_nan() {
            Some(Ordering::Greater) // everything else > NaN
        } else {
            self.score.partial_cmp(&other.score)
        }
    }
}

impl Ord for ZSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
