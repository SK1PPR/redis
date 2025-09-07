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

    // Sorted Set Commands
    ZADD(String, f64, String),
    ZRANK(String, String),
    ZRANGE(String, i64, i64),
    ZCARD(String),
    ZSCORE(String, String),
    ZREM(String, String),

    // Stream commands
    TYPE(String),
    XADD(String, Option<String>, Vec<(String, String)>),
    XRANGE(String, String, String),
    XREAD(Option<u64>, Vec<(String, String)>),

    // Geo Spatial Commands
    GEOADD(String, f64, f64, String),
    GEOPOS(String, Vec<String>),
    GEODIST(String, String, String),
    GEOSEARCH(String, f64, f64, bool, f64, String), // bool: use_radius, last parameter is unit

    // Replication Commands
    CONFIG(String, String),
    KEYS(String),
    INFO(String),

    // Pub/Sub Commands
    SUBSCRIBE(String),
    PUBLISH(String, String),
    UNSUBSCRIBE(String),

    // Replication Commands
    REPLCONF(String, String),
    PSYNC(String, String)
}

impl RedisCommand {
    pub fn to_string(&self) -> String {
        match self {
            RedisCommand::Ping(_) => "ping".to_string(),
            RedisCommand::Echo(_) => "echo".to_string(),
            RedisCommand::Get(_) => "get".to_string(),
            RedisCommand::Set(_, _) => "set".to_string(),
            RedisCommand::SetWithExpiry(_, _, _) => "set".to_string(),
            RedisCommand::Del(_) => "del".to_string(),
            RedisCommand::Exists(_) => "exists".to_string(),
            RedisCommand::RPUSH(_, _) => "rpush".to_string(),
            RedisCommand::LRANGE(_, _, _) => "lrange".to_string(),
            RedisCommand::LPUSH(_, _) => "lpush".to_string(),
            RedisCommand::LLEN(_) => "llen".to_string(),
            RedisCommand::LPOP(_, _) => "lpop".to_string(),
            RedisCommand::BLPOP(_, _) => "blpop".to_string(),
            RedisCommand::BRPOP(_, _) => "brpop".to_string(),
            RedisCommand::INCR(_) => "incr".to_string(),
            RedisCommand::MULTI => "multi".to_string(),
            RedisCommand::EXEC => "exec".to_string(),
            RedisCommand::DISCARD => "discard".to_string(),
            RedisCommand::ZADD(_, _, _) => "zadd".to_string(),
            RedisCommand::ZRANK(_, _) => "zrank".to_string(),
            RedisCommand::ZRANGE(_, _, _) => "zrange".to_string(),
            RedisCommand::ZCARD(_) => "zcard".to_string(),
            RedisCommand::ZSCORE(_, _) => "zscore".to_string(),
            RedisCommand::ZREM(_, _) => "zrem".to_string(),
            RedisCommand::TYPE(_) => "type".to_string(),
            RedisCommand::XADD(_, _, _) => "xadd".to_string(),
            RedisCommand::XRANGE(_, _, _) => "xrange".to_string(),
            RedisCommand::XREAD(_, _) => "xread".to_string(),
            RedisCommand::GEOADD(_, _, _, _) => "geoadd".to_string(),
            RedisCommand::GEOPOS(_, _) => "geopos".to_string(),
            RedisCommand::GEODIST(_, _, _) => "geodist".to_string(),
            RedisCommand::GEOSEARCH(_, _, _, _, _, _) => "geosearch".to_string(),
            RedisCommand::CONFIG(_, _) => "config".to_string(),
            RedisCommand::KEYS(_) => "keys".to_string(),
            RedisCommand::INFO(_) => "info".to_string(),
            RedisCommand::SUBSCRIBE(_) => "subscribe".to_string(),
            RedisCommand::PUBLISH(_, _) => "publish".to_string(),
            RedisCommand::UNSUBSCRIBE(_) => "unsubscribe".to_string(),
            RedisCommand::REPLCONF(_, _) => "replconf".to_string(),
            RedisCommand::PSYNC(_, _) => "psync".to_string(),
        }
    }
}
