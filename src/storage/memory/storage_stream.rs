use std::time::SystemTime;

use super::{MemoryStorage, StorageStream, Unit};
use crate::storage::stream_member::{StreamId, StreamMember, EMPTY_STREAM_ID};

impl StorageStream for MemoryStorage {
    fn xadd(
        &mut self,
        key: String,
        id: String,
        fields: Vec<(String, String)>,
    ) -> Result<String, String> {
        log::debug!("XADD called for key '{}'", key);
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_stream() {
                    log::debug!("Key '{}' has expired or is not a stream", key);
                    return Err("Key does not exist or is not a stream".to_string());
                }
                if let Some(stream) = u.implementation.as_stream_mut() {
                    let top_id = if let Some(last) = stream.last() {
                        last.id.clone()
                    } else {
                        EMPTY_STREAM_ID.clone()
                    };
                    let entry_id = generate_next_id(&top_id, &id);
                    if entry_id <= top_id {
                        log::debug!(
                            "Provided ID '{}' is not greater than the last ID '{}'",
                            id,
                            top_id.to_string()
                        );
                        if entry_id == EMPTY_STREAM_ID {
                            return Err(
                                "The ID specified in XADD must be greater than 0-0".to_string()
                            );
                        }
                        return Err("The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                    }
                    stream.push(StreamMember {
                        id: entry_id.clone(),
                        fields,
                    });
                    Ok(entry_id.to_string())
                } else {
                    return Err("Key does not exist or is not a stream".to_string());
                }
            }
            None => {
                let mut new_stream = Vec::new();
                let entry_id = generate_next_id(&EMPTY_STREAM_ID, &id);
                if entry_id <= EMPTY_STREAM_ID {
                    log::debug!("Invalid stream ID '{}'", id);
                    return Err("The ID specified in XADD must be greater than 0-0".to_string());
                }
                new_stream.push(StreamMember {
                    id: entry_id.clone(),
                    fields,
                });
                self.storage.insert(key, Unit::new_stream(new_stream, None));
                Ok(entry_id.to_string())
            }
        }
    }

    fn xrange(
        &self,
        key: &str,
        start: String,
        end: String,
    ) -> Option<Vec<(String, Vec<(String, String)>)>> {
        log::debug!("XRANGE called for key '{}'", key);
        let unit = self.storage.get(key)?;
        if unit.is_expired() || !unit.implementation.is_stream() {
            log::debug!("Key '{}' has expired or is not a stream", key);
            return None;
        }
        let stream = unit.implementation.as_stream()?;
        let start_id = generate_query_id(&start);
        let end_id = generate_query_id(&end);

        let mut result = Vec::new();
        for member in stream {
            if member.id >= start_id && member.id <= end_id {
                result.push((member.id.to_string(), member.fields.clone()));
            }
        }
        Some(result)
    }
}

fn generate_next_id(last_id: &StreamId, input: &str) -> StreamId {
    if input == "*" {
        return StreamId {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            sequence: 0,
        };
    }

    let (first, last) = input.split_once('-').unwrap_or(("0", "0"));

    if last == "*" {
        let first = first.parse::<u64>().unwrap_or(0);
        return StreamId {
            timestamp: first,
            sequence: if first == last_id.timestamp {
                last_id.sequence + 1
            } else {
                0
            },
        };
    }

    StreamId {
        timestamp: first.parse().unwrap_or(0),
        sequence: last.parse().unwrap_or(0),
    }
}

fn generate_query_id(input: &str) -> StreamId {
    if input == "-" || input == "+" {
        return StreamId {
            timestamp: if input == "-" { 0 } else { u64::MAX },
            sequence: if input == "-" { 0 } else { u64::MAX },
        };
    }

    if !input.contains("-") {
        // Only timestamp provided
        let timestamp = input.parse::<u64>().unwrap_or(0);
        return StreamId {
            timestamp,
            sequence: 0,
        };
    }

    let (first, last) = input.split_once('-').unwrap_or(("0", "0"));

    StreamId {
        timestamp: first.parse().unwrap_or(0),
        sequence: last.parse().unwrap_or(0),
    }
}
