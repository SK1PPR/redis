use std::collections::BTreeSet;
use super::zset_member::ZSetMember;
use super::stream_member::StreamMember;

#[derive(Debug, Clone)]
pub enum Implementation {
    STRING(String),
    LIST(Vec<String>),
    STREAM(Vec<StreamMember>),
    SET,
    ZSET(BTreeSet<ZSetMember>), // (score, member)
    HASH,
}

impl Implementation {
    pub fn is_string(&self) -> bool {
        matches!(self, Implementation::STRING(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Implementation::LIST(_))
    }

    pub fn is_stream(&self) -> bool {
        matches!(self, Implementation::STREAM(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Implementation::SET)
    }

    pub fn is_zset(&self) -> bool {
        matches!(self, Implementation::ZSET(_))
    }

    pub fn is_hash(&self) -> bool {
        matches!(self, Implementation::HASH)
    }

    pub fn as_string(&self) -> Option<&String> {
        if let Implementation::STRING(ref s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_list(&self) -> Option<&Vec<String>> {
        if let Implementation::LIST(ref l) = self {
            Some(l)
        } else {
            None
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut Vec<String>> {
        if let Implementation::LIST(ref mut l) = self {
            Some(l)
        } else {
            None
        }
    }

    pub fn extend_list(&mut self, values: Vec<String>) {
        if let Implementation::LIST(ref mut l) = self {
            l.extend(values);
        } else {
            panic!("Cannot extend non-list implementation");
        }
    }

    pub fn get_list_length(&self) -> usize {
        if let Implementation::LIST(ref l) = self {
            l.len()
        } else {
            panic!("Cannot get length of non-list implementation");
        }
    }

    pub fn as_zset(&self) -> Option<&BTreeSet<ZSetMember>> {
        if let Implementation::ZSET(ref z) = self {
            Some(z)
        } else {
            None
        }
    }

    pub fn as_zset_mut(&mut self) -> Option<&mut BTreeSet<ZSetMember>> {
        if let Implementation::ZSET(ref mut z) = self {
            Some(z)
        } else {
            None
        }
    }

    pub fn as_stream(&self) -> Option<&Vec<StreamMember>> {
        if let Implementation::STREAM(ref s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut Vec<StreamMember>> {
        if let Implementation::STREAM(ref mut s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn is_zset_geo(&self) -> bool {
        if let Implementation::ZSET(ref zset) = self {
            if let Some(first) = zset.iter().next() {
                return first.is_geo();
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct Unit {
    pub implementation: Implementation,
    pub expiry: Option<u128>,
}

impl Unit {
    pub fn new_string(value: String, expiry: Option<u128>) -> Self {
        Unit {
            implementation: Implementation::STRING(value),
            expiry,
        }
    }

    pub fn new_list(value: Vec<String>, expiry: Option<u128>) -> Self {
        Unit {
            implementation: Implementation::LIST(value),
            expiry,
        }
    }

    pub fn new_zset(value: BTreeSet<ZSetMember>, expiry: Option<u128>) -> Self {
        Unit {
            implementation: Implementation::ZSET(value),
            expiry,
        }
    }

    pub fn new_stream(value: Vec<StreamMember>, expiry: Option<u128>) -> Self {
        Unit {
            implementation: Implementation::STREAM(value),
            expiry,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            return now > expiry;
        }
        false
    }
}
