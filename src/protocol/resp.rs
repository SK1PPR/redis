use std::str;

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}

pub struct RespParser;

impl RespParser {
    /// Parse RESP commands from a buffer
    /// Returns (parsed_commands, bytes_consumed)
    pub fn parse_commands(buffer: &[u8]) -> (Vec<Vec<String>>, usize) {
        let mut commands = Vec::new();
        let mut pos = 0;
        
        while pos < buffer.len() {
            match Self::parse_single_command(&buffer[pos..]) {
                Some((command, consumed)) => {
                    commands.push(command);
                    pos += consumed;
                }
                None => break, // Incomplete command, wait for more data
            }
        }
        
        (commands, pos)
    }
    
    /// Parse a single RESP command
    /// Returns (command_args, bytes_consumed) if successful
    fn parse_single_command(buffer: &[u8]) -> Option<(Vec<String>, usize)> {
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
        
        // Check if we have enough data
        if pos + length + 2 > buffer.len() {
            return None; // Incomplete
        }
        
        // Extract string
        let string_data = &buffer[pos..pos + length];
        let string = String::from_utf8_lossy(string_data).to_string();
        pos += length;
        
        // Skip \r\n
        if pos + 1 < buffer.len() && buffer[pos] == b'\r' && buffer[pos + 1] == b'\n' {
            pos += 2;
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
    
    // Simple implementations for other types (mainly for completeness)
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