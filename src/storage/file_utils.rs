use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::storage::Unit;

#[derive(Debug)]
enum FileStage {
    Header,
    Metadata,
    Data,
    End,
}

pub struct FileUtils;

impl FileUtils {
    // Read a length-encoded value (fixed implementation)
    fn read_length_encoded(buffer: &[u8], pos: &mut usize) -> Option<usize> {
        if *pos >= buffer.len() {
            return None;
        }

        let first_byte = buffer[*pos];
        *pos += 1;

        match first_byte >> 6 {
            0b00 => {
                // The length is the remaining 6 bits
                Some((first_byte & 0x3F) as usize)
            }
            0b01 => {
                // The length is the next 14 bits (6 bits + next byte), big-endian
                if *pos >= buffer.len() {
                    return None;
                }
                let next_byte = buffer[*pos];
                *pos += 1;
                Some((((first_byte & 0x3F) as usize) << 8) | (next_byte as usize))
            }
            0b10 => {
                // The length is the next 4 bytes, big-endian
                if *pos + 4 > buffer.len() {
                    return None;
                }
                let len = ((buffer[*pos] as usize) << 24)
                    | ((buffer[*pos + 1] as usize) << 16)
                    | ((buffer[*pos + 2] as usize) << 8)
                    | (buffer[*pos + 3] as usize);
                *pos += 4;
                Some(len)
            }
            0b11 => {
                // Special string encoding - should not be handled here
                // Move position back since this will be handled by string encoding
                *pos -= 1;
                None
            }
            _ => {
                // This should never happen as we're only shifting 6 bits right,
                // but Rust requires exhaustive patterns
                log::error!(
                    "Unexpected bit pattern in length encoding: {:#b}",
                    first_byte >> 6
                );
                None
            }
        }
    }

    // Read a string-encoded value (improved implementation)
    fn read_string_encoded(buffer: &[u8], pos: &mut usize) -> Option<String> {
        if *pos >= buffer.len() {
            return None;
        }

        let first_byte = buffer[*pos];

        // Check if it's a special string encoding (starts with 0b11)
        if first_byte >> 6 == 0b11 {
            *pos += 1;
            match first_byte & 0x3F {
                0x00 => {
                    // 8-bit integer
                    if *pos >= buffer.len() {
                        return None;
                    }
                    let value = buffer[*pos] as i8;
                    *pos += 1;
                    Some(value.to_string())
                }
                0x01 => {
                    // 16-bit integer (little-endian)
                    if *pos + 2 > buffer.len() {
                        return None;
                    }
                    let value = (buffer[*pos] as u16) | ((buffer[*pos + 1] as u16) << 8);
                    *pos += 2;
                    Some((value as i16).to_string())
                }
                0x02 => {
                    // 32-bit integer (little-endian)
                    if *pos + 4 > buffer.len() {
                        return None;
                    }
                    let value = (buffer[*pos] as u32)
                        | ((buffer[*pos + 1] as u32) << 8)
                        | ((buffer[*pos + 2] as u32) << 16)
                        | ((buffer[*pos + 3] as u32) << 24);
                    *pos += 4;
                    Some((value as i32).to_string())
                }
                _ => {
                    log::error!(
                        "Unsupported special string encoding: {:#x}",
                        first_byte & 0x3F
                    );
                    None
                }
            }
        } else {
            // Regular string with length encoding
            let len = Self::read_length_encoded(buffer, pos)?;

            if *pos + len > buffer.len() {
                log::error!("String length {} exceeds buffer bounds", len);
                return None;
            }

            let result = String::from_utf8_lossy(&buffer[*pos..*pos + len]).to_string();
            *pos += len;

            Some(result)
        }
    }

    // Parse a key-value pair with optional expiry
    fn parse_key_value_pair(
        buffer: &[u8],
        pos: &mut usize,
        value_type: u8,
        expiry_timestamp: Option<u64>,
        db: &mut HashMap<String, Unit>,
    ) -> Option<()> {
        // Parse key
        let key = Self::read_string_encoded(buffer, pos)?;

        // Parse value based on the value type
        let value = match value_type {
            0x00 => {
                // String type
                let string_value = Self::read_string_encoded(buffer, pos)?;
                string_value
            }
            // Add other value types as needed (lists, sets, etc.)
            _ => {
                log::warn!("Unsupported value type: {:#x}, skipping entry", value_type);
                return None;
            }
        };

        // Convert expiry timestamp to system time if present
        let expiry_systime = expiry_timestamp
            .map(|timestamp| UNIX_EPOCH + std::time::Duration::from_millis(timestamp));

        // Check if the key has expired
        if let Some(expiry_time) = expiry_systime {
            if let Ok(current_time) = SystemTime::now().duration_since(UNIX_EPOCH) {
                if let Ok(expiry_duration) = expiry_time.duration_since(UNIX_EPOCH) {
                    if current_time > expiry_duration {
                        log::debug!("Key '{}' has expired, skipping", key);
                        return Some(()); // Skip expired keys
                    }
                }
            }
        }

        // Convert SystemTime to u128 (milliseconds since epoch) for Unit::new_string
        let expiry_u128 = expiry_timestamp.map(|ts| ts as u128);
        db.insert(key.clone(), Unit::new_string(value, expiry_u128));
        log::debug!(
            "Parsed key-value pair: '{}' with expiry: {:?}",
            key,
            expiry_u128
        );
        Some(())
    }

    pub fn validate_db_file(dir: &str, dbfilename: &str) -> bool {
        // Check if the directory exists
        let dir_path = std::path::Path::new(dir);
        if !dir_path.exists() || !dir_path.is_dir() {
            log::error!("Directory '{}' does not exist or is not a directory", dir);
            return false;
        }

        // Check if the dbfilename is a valid filename (not a directory)
        let db_path = dir_path.join(dbfilename);
        if db_path.exists() && db_path.is_dir() {
            log::error!("'{}' is a directory, expected a file", db_path.display());
            return false;
        }

        // Check if dbfilename ends with .rdb
        if !dbfilename.ends_with(".rdb") {
            log::error!("Database filename '{}' must end with .rdb", dbfilename);
            return false;
        }

        true
    }

    pub fn construct_db_from_file(dir: &str, dbfilename: &str) -> Option<HashMap<String, Unit>> {
        let file_path = std::path::Path::new(dir).join(dbfilename);
        if !file_path.exists() || !file_path.is_file() {
            log::warn!("RDB file does not exist: {}", file_path.display());
            return Some(HashMap::new()); // Return empty database if file doesn't exist
        }

        // Read file into buffer
        let file = File::open(&file_path).ok()?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        if reader.read_to_end(&mut buffer).is_err() {
            log::error!("Failed to read RDB file: {}", file_path.display());
            return None;
        }

        let mut db = HashMap::new();
        let mut position = 0;
        let mut stage = FileStage::Header;
        let mut pending_expiry: Option<u64> = None;

        log::debug!(
            "Starting RDB file parsing, file size: {} bytes",
            buffer.len()
        );

        while position < buffer.len() {
            match stage {
                FileStage::Header => {
                    // Check for RDB header (9 bytes: "REDIS" + 4-byte version)
                    if position + 9 > buffer.len() {
                        log::error!("RDB file too short for header");
                        return None;
                    }

                    if &buffer[position..position + 5] != b"REDIS" {
                        log::error!("Invalid RDB magic string");
                        return None;
                    }

                    position += 5;
                    let version =
                        std::str::from_utf8(&buffer[position..position + 4]).unwrap_or("unknown");
                    position += 4;

                    log::debug!("Header parsed successfully, version: {}", version);
                    stage = FileStage::Metadata;
                }
                FileStage::Metadata => {
                    // Parse metadata sections
                    if position >= buffer.len() {
                        stage = FileStage::End;
                        continue;
                    }

                    let marker = buffer[position];
                    match marker {
                        0xFA => {
                            // Metadata subsection marker
                            position += 1;

                            // Read metadata name
                            let name = Self::read_string_encoded(&buffer, &mut position)?;
                            // Read metadata value
                            let value = Self::read_string_encoded(&buffer, &mut position)?;
                            log::debug!("Metadata: {} = {}", name, value);
                        }
                        0xFE => {
                            // Start of database section
                            stage = FileStage::Data;
                        }
                        0xFF => {
                            // End of file
                            stage = FileStage::End;
                        }
                        _ => {
                            // No more metadata, assume we're moving to data section
                            stage = FileStage::Data;
                            continue; // Don't increment position
                        }
                    }
                }
                FileStage::Data => {
                    if position >= buffer.len() {
                        stage = FileStage::End;
                        continue;
                    }

                    let marker = buffer[position];
                    position += 1;

                    match marker {
                        0xFF => {
                            // End of RDB file marker
                            log::debug!("Found end of file marker");
                            stage = FileStage::End;
                        }
                        0xFE => {
                            // Start of database subsection
                            let db_index = Self::read_length_encoded(&buffer, &mut position)?;
                            log::debug!("Switching to database: {}", db_index);
                        }
                        0xFB => {
                            // Hash table size information
                            let hash_table_size =
                                Self::read_length_encoded(&buffer, &mut position)?;
                            let expire_hash_size =
                                Self::read_length_encoded(&buffer, &mut position)?;
                            log::debug!(
                                "Hash table size: {}, expire hash table size: {}",
                                hash_table_size,
                                expire_hash_size
                            );
                        }
                        0xFC => {
                            // Expiry time in milliseconds (8 bytes, little-endian)
                            if position + 8 > buffer.len() {
                                log::error!(
                                    "Invalid expiry format (milliseconds) - buffer too short"
                                );
                                return None;
                            }

                            let mut timestamp: u64 = 0;
                            for i in 0..8 {
                                timestamp |= (buffer[position + i] as u64) << (i * 8);
                            }
                            position += 8;
                            pending_expiry = Some(timestamp);
                            log::debug!("Set pending expiry timestamp (ms): {}", timestamp);
                        }
                        0xFD => {
                            // Expiry time in seconds (4 bytes, little-endian)
                            if position + 4 > buffer.len() {
                                log::error!("Invalid expiry format (seconds) - buffer too short");
                                return None;
                            }

                            let mut timestamp: u32 = 0;
                            for i in 0..4 {
                                timestamp |= (buffer[position + i] as u32) << (i * 8);
                            }
                            position += 4;
                            // Convert seconds to milliseconds
                            pending_expiry = Some(timestamp as u64 * 1000);
                            log::debug!(
                                "Set pending expiry timestamp (s): {} ({}ms)",
                                timestamp,
                                timestamp as u64 * 1000
                            );
                        }
                        _ => {
                            // Regular key-value pair (marker is the value type)
                            Self::parse_key_value_pair(
                                &buffer,
                                &mut position,
                                marker,
                                pending_expiry.take(), // Use and clear pending expiry
                                &mut db,
                            )?;
                        }
                    }
                }
                FileStage::End => {
                    // Handle checksum if present
                    if position + 8 <= buffer.len() {
                        log::debug!("Found CRC64 checksum at end of file");
                    }
                    log::debug!("RDB parsing complete. Loaded {} keys", db.len());
                    break;
                }
            }
        }

        log::info!("Successfully loaded {} keys from RDB file", db.len());
        Some(db)
    }

    pub fn get_db_as_file() -> Vec<u8> {
        // Convert hex string to Vec<u8>
        let hex_string = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let mut bytes = Vec::with_capacity(hex_string.len() / 2);
        
        for i in (0..hex_string.len()).step_by(2) {
            if i + 2 > hex_string.len() {
            break;
            }
            let byte_str = &hex_string[i..i+2];
            match u8::from_str_radix(byte_str, 16) {
            Ok(byte) => bytes.push(byte),
            Err(_) => {
                log::error!("Invalid hex string at position {}: {}", i, byte_str);
                return Vec::new();
            }
            }
        }
        
        bytes
    }
}
