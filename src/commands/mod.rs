pub mod parser;
pub mod executor;
pub mod response;

pub use parser::CommandParser;
pub use executor::{CommandExecutor, RedisCommandExecutor};
pub use response::RedisResponse;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommand {
    Ping(Option<String>),
    Echo(String),
    Get(String),
    Set(String, String),
    SetWithExpiry(String, String, u128),
    Del(Vec<String>),
    Exists(Vec<String>),
    RPUSH(String, Vec<String>),
    LRANGE(String, i64, i64),
    LPUSH(String, Vec<String>),
    LLEN(String),
    LPOP(String, Option<i64>),
    BLPOP(Vec<String>, i64),
    BRPOP(Vec<String>, i64),
}