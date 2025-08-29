use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Member {
    pub score: f64,
    pub member: String,
}

impl Member {
    fn same_score(&self, other: &Self) -> bool {
        (self.score - other.score).abs() < f64::EPSILON
    }

    fn is_nan(&self) -> bool {
        self.score.is_nan()
    }
}

impl PartialEq for Member {
    fn eq(&self, other: &Self) -> bool {
        self.same_score(other) && self.member == other.member
    }
}

impl Eq for Member {}

impl PartialOrd for Member {
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

impl Ord for Member {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
