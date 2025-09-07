use std::str;

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
    RdbData(Vec<u8>), // Special case for RDB data
}

pub struct RespParser {
    /// Track if we're currently expecting RDB data
    expecting_rdb: bool,
    /// Size of RDB data we're expecting (if known)
    expected_rdb_size: Option<usize>,
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            expecting_rdb: true,
            expected_rdb_size: None,
        }
    }
    
    /// Set RDB expectation mode (call this when you receive FULLRESYNC)
    pub fn set_expecting_rdb(&mut self, size: Option<usize>) {
        self.expecting_rdb = true;
        self.expected_rdb_size = size;
    }
    
    /// Parse RESP commands from a buffer
    /// Returns (parsed_commands, bytes_consumed)
    pub fn parse_commands(&mut self, buffer: &[u8]) -> (Vec<Vec<String>>, usize) {
        let mut commands = Vec::new();
        let mut pos = 0;
        
        // Handle RDB data if we're expecting it
        if self.expecting_rdb {
            if let Some((rdb_data, consumed)) = self.try_parse_rdb_data(&buffer[pos..]) {
                // We got the RDB data, now continue with normal parsing
                self.expecting_rdb = false;
                self.expected_rdb_size = None;
                pos += consumed;
                
                // Store RDB data as a special command type
                commands.push(vec!["__RDB_DATA__".to_string(), 
                                 format!("({} bytes)", rdb_data.len())]);
            } else {
                // Still waiting for complete RDB data
                return (commands, 0);
            }
        }
        
        while pos < buffer.len() {
            match self.parse_single_command(&buffer[pos..]) {
                Some((command, consumed)) => {
                    // Check if this is a FULLRESYNC command that will be followed by RDB data
                    if command.len() >= 1 && command[0].to_uppercase() == "FULLRESYNC" {
                        self.expecting_rdb = true;
                    }
                    
                    commands.push(command);
                    pos += consumed;
                }
                None => break, // Incomplete command, wait for more data
            }
        }
        
        (commands, pos)
    }
    
    /// Try to parse RDB data
    fn try_parse_rdb_data(&self, buffer: &[u8]) -> Option<(Vec<u8>, usize)> {
        if buffer.is_empty() {
            return None;
        }
        
        // RDB data comes as a bulk string: $<length>\r\n<data>
        if buffer[0] != b'$' {
            return None;
        }
        
        let mut pos = 1; // Skip '$'
        
        // Parse length
        let (length, consumed) = Self::parse_integer_from_buffer(&buffer[pos..])?;
        pos += consumed;
        
        if length < 0 {
            return Some((vec![], pos)); // Null bulk string
        }
        
        let length = length as usize;
        
        // Check if we have enough data for the complete RDB file
        if pos + length > buffer.len() {
            return None; // Incomplete RDB data
        }
        
        // Extract RDB data (raw bytes, not UTF-8)
        let rdb_data = buffer[pos..pos + length].to_vec();
        pos += length;
        
        // Note: RDB data might not end with \r\n as it's binary data
        // The \r\n terminator might be embedded in the binary data
        
        Some((rdb_data, pos))
    }
    
    /// Parse a single RESP command
    /// Returns (command_args, bytes_consumed) if successful
    fn parse_single_command(&self, buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        if buffer.is_empty() {
            return None;
        }
        
        match buffer[0] {
            b'*' => Self::parse_array(buffer),
            b'+' => Self::parse_simple_string_as_command(buffer),
            b':' => Self::parse_integer_as_command(buffer),
            b'$' => Self::parse_bulk_string_as_command(buffer),
            b'-' => Self::parse_error_as_command(buffer),
            _ => {
                // Try to parse as inline command (for telnet compatibility)
                Self::parse_inline_command(buffer)
            }
        }
    }
    
    fn parse_array(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let mut pos = 1; // Skip '*'
        
        // Parse array length
        let (length, consumed) = Self::parse_integer_from_buffer(&buffer[pos..])?;
        pos += consumed;
        
        if length < 0 {
            return Some((vec![], pos)); // Null array
        }
        
        let mut elements = Vec::new();
        
        for _ in 0..length {
            if pos >= buffer.len() {
                return None; // Incomplete
            }
            
            match buffer[pos] {
                b'$' => {
                    let (bulk_str, consumed) = Self::parse_bulk_string_value(&buffer[pos..])?;
                    if let Some(s) = bulk_str {
                        elements.push(s);
                    }
                    pos += consumed;
                }
                b'+' => {
                    let (simple_str, consumed) = Self::parse_simple_string_value(&buffer[pos..])?;
                    elements.push(simple_str);
                    pos += consumed;
                }
                b':' => {
                    let pos_before = pos + 1; // Skip ':'
                    let (integer, consumed) = Self::parse_integer_from_buffer(&buffer[pos_before..])?;
                    elements.push(integer.to_string());
                    pos = pos_before + consumed;
                }
                _ => return None, // Unsupported type in command array
            }
        }
        
        Some((elements, pos))
    }
    
    fn parse_bulk_string_value(buffer: &[u8]) -> Option<(Option<String>, usize)> {
        let mut pos = 1; // Skip '$'
        
        // Parse length
        let (length, consumed) = Self::parse_integer_from_buffer(&buffer[pos..])?;
        pos += consumed;
        
        if length < 0 {
            return Some((None, pos)); // Null bulk string
        }
        
        let length = length as usize;
        
        // Check if we have enough data including the trailing \r\n
        if pos + length + 2 > buffer.len() {
            return None; // Incomplete
        }
        
        // Extract string
        let string_data = &buffer[pos..pos + length];
        let string = String::from_utf8_lossy(string_data).to_string();
        pos += length;
        
        // Skip \r\n (bulk strings should always end with \r\n)
        if pos + 1 < buffer.len() && buffer[pos] == b'\r' && buffer[pos + 1] == b'\n' {
            pos += 2;
        } else {
            // This might be malformed or incomplete
            return None;
        }
        
        Some((Some(string), pos))
    }
    
    fn parse_simple_string_value(buffer: &[u8]) -> Option<(String, usize)> {
        let mut pos = 1; // Skip '+'
        
        // Find \r\n
        while pos + 1 < buffer.len() {
            if buffer[pos] == b'\r' && buffer[pos + 1] == b'\n' {
                let string = String::from_utf8_lossy(&buffer[1..pos]).to_string();
                return Some((string, pos + 2));
            }
            pos += 1;
        }
        
        None // Incomplete
    }
    
    fn parse_integer_from_buffer(buffer: &[u8]) -> Option<(i64, usize)> {
        let mut pos = 0;
        
        // Find \r\n
        while pos + 1 < buffer.len() {
            if buffer[pos] == b'\r' && buffer[pos + 1] == b'\n' {
                let number_str = str::from_utf8(&buffer[..pos]).ok()?;
                let number = number_str.parse::<i64>().ok()?;
                return Some((number, pos + 2));
            }
            pos += 1;
        }
        
        None
    }
    
    // Simple implementations for other types
    fn parse_simple_string_as_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let (string, consumed) = Self::parse_simple_string_value(buffer)?;
        Some((vec![string], consumed))
    }
    
    fn parse_integer_as_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let pos = 1; // Skip ':'
        let (integer, consumed) = Self::parse_integer_from_buffer(&buffer[pos..])?;
        Some((vec![integer.to_string()], pos + consumed))
    }
    
    fn parse_bulk_string_as_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let (bulk_str, consumed) = Self::parse_bulk_string_value(buffer)?;
        match bulk_str {
            Some(s) => Some((vec![s], consumed)),
            None => Some((vec![], consumed)),
        }
    }
    
    fn parse_error_as_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let mut pos = 1; // Skip '-'
        
        // Find \r\n
        while pos + 1 < buffer.len() {
            if buffer[pos] == b'\r' && buffer[pos + 1] == b'\n' {
                let error = String::from_utf8_lossy(&buffer[1..pos]).to_string();
                return Some((vec!["ERROR".to_string(), error], pos + 2));
            }
            pos += 1;
        }
        
        None
    }
    
    // Parse inline commands (for telnet compatibility like "GET key")
    fn parse_inline_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
        let mut pos = 0;
        
        // Find \r\n or \n
        while pos < buffer.len() {
            if buffer[pos] == b'\n' || (pos + 1 < buffer.len() && buffer[pos] == b'\r' && buffer[pos + 1] == b'\n') {
                let line_end = if buffer[pos] == b'\n' { pos } else { pos };
                let line = String::from_utf8_lossy(&buffer[..line_end]);
                let args: Vec<String> = line.trim().split_whitespace().map(|s| s.to_string()).collect();
                
                let consumed = if buffer[pos] == b'\n' { pos + 1 } else { pos + 2 };
                return Some((args, consumed));
            }
            pos += 1;
        }
        
        None // Incomplete line
    }
}