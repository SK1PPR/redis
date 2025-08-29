#[derive(Debug, Clone)]
pub struct StreamMember {
    pub id: StreamId,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub timestamp: u64,
    pub sequence: u64,
}

impl StreamId {
    pub fn new(id: &str) -> Self {
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return StreamId {
                sequence: 0,
                timestamp: 0,
            };
        }
        let timestamp = parts[0].parse::<u64>().unwrap_or(0);
        let sequence = parts[1].parse::<u64>().unwrap_or(0);
        StreamId {
            timestamp,
            sequence,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.timestamp == 0 && self.sequence == 0
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}", self.timestamp, self.sequence)
    }
}

pub const EMPTY_STREAM_ID: StreamId = StreamId {
    timestamp: 0,
    sequence: 0,
};
