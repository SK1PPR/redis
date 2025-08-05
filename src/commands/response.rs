use std::fmt;

#[derive(Debug, Clone)]
pub enum RedisResponse {
    SimpleString(String),
    BulkString(Option<String>), // None represents null
    Integer(i64),
    Array(Vec<RedisResponse>),
    Error(String),
}

impl RedisResponse {
    pub fn to_resp(&self) -> String {
        match self {
            RedisResponse::SimpleString(s) => format!("+{}\r\n", s),
            RedisResponse::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
            RedisResponse::BulkString(None) => "$-1\r\n".to_string(),
            RedisResponse::Integer(i) => format!(":{}\r\n", i),
            RedisResponse::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len());
                for item in arr {
                    result.push_str(&item.to_resp());
                }
                result
            }
            RedisResponse::Error(e) => format!("-ERR {}\r\n", e),
        }
    }
    
    pub fn ok() -> Self {
        RedisResponse::SimpleString("OK".to_string())
    }
    
    pub fn pong() -> Self {
        RedisResponse::SimpleString("PONG".to_string())
    }
    
    pub fn nil() -> Self {
        RedisResponse::BulkString(None)
    }
    
    pub fn error(msg: &str) -> Self {
        RedisResponse::Error(msg.to_string())
    }
}

impl fmt::Display for RedisResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_resp())
    }
}