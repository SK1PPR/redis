use super::{MemoryStorage, Storage, StorageStream, Unit};

impl StorageStream for MemoryStorage {
    fn xadd(&mut self, key: String, id: String, fields: Vec<(String, String)>) -> String {
        log::debug!("XADD called for key '{}'", key);
        let unit = self.storage.entry(key.clone()).or_insert_with(|| {
            log::debug!("Key '{}' does not exist. Creating new STREAM.", key);
            Unit::new_stream(None)
        });

        if !unit.implementation.is_stream() {
            log::debug!("Key '{}' is not a STREAM. XADD failed.", key);
            return "-WRONGTYPE Operation against a key holding the wrong kind of value".to_string();
        }

        let entry_id = if id == "*" {
            // Auto-generate ID (for simplicity, using current timestamp)
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            format!("{}-0", timestamp)
        } else {
            id
        };

        log::debug!(
            "Added entry with ID '{}' to STREAM '{}': {:?}",
            entry_id,
            key,
            fields
        );

        entry_id
    }
}