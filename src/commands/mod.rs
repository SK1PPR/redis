pub mod executor;
pub mod parser;
pub mod response;

pub use executor::{CommandExecutor, RedisCommandExecutor};
pub use parser::CommandParser;
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
    BLPOP(Vec<String>, u64),
    BRPOP(Vec<String>, u64),
    INCR(String),
    MULTI,
    EXEC,
    DISCARD,
    ZADD(String, f64, String),
    ZRANK(String, String),
    ZRANGE(String, i64, i64),
    ZCARD(String),
    ZSCORE(String, String),
    ZREM(String, String),
    TYPE(String),
    XADD(String, Option<String>, Vec<(String, String)>),
    XRANGE(String, String, String),
    XREAD(Option<u64>,Vec<(String, String)>),
    GEOADD(String, f64, f64, String),
    GEOPOS(String, Vec<String>),
    GEODIST(String, String, String),
    GEOSEARCH(String, f64, f64, bool, f64, String), // bool: use_radius, last parameter is unit
    CONFIG(String, String),
    KEYS(String),
    INFO(String),
}
