use crate::storage::stream_member::{StreamId, StreamMember, EMPTY_STREAM_ID};

use super::{MemoryStorage, StorageStream, Unit};

impl StorageStream for MemoryStorage {
    fn xadd(&mut self, key: String, id: String, fields: Vec<(String, String)>) -> Option<String> {
        log::debug!("XADD called for key '{}'", key);
        let unit = self.storage.get_mut(&key);
        match unit {
            Some(u) => {
                if u.is_expired() || !u.implementation.is_stream() {
                    log::debug!("Key '{}' has expired or is not a stream", key);
                    return None;
                }
                if let Some(stream) = u.implementation.as_stream_mut() {
                    let top_id = if let Some(last) = stream.last() {
                        last.id.clone()
                    } else {
                        EMPTY_STREAM_ID.clone()
                    };
                    let entry_id = StreamId::new(&id);
                    if entry_id <= top_id {
                        log::debug!(
                            "Provided ID '{}' is not greater than the last ID '{}'",
                            id,
                            top_id.to_string()
                        );
                        return None;
                    }
                    stream.push(StreamMember {
                        id: entry_id.clone(),
                        fields,
                    });
                    Some(entry_id.to_string())
                } else {
                    return None;
                }
            }
            None => {
                let mut new_stream = Vec::new();
                let entry_id = StreamId::new(&id);
                if entry_id <= EMPTY_STREAM_ID {
                    log::debug!("Invalid stream ID '{}'", id);
                    return None;
                }
                new_stream.push(StreamMember {
                    id: entry_id.clone(),
                    fields,
                });
                self.storage.insert(key, Unit::new_stream(None));
                Some(entry_id.to_string())
            }
        }
    }
}
