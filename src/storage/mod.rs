pub mod memory;
mod stream_member;
pub mod unit;
mod zset_member;
mod file_utils;
pub mod repl_config;

pub use memory::MemoryStorage;
pub use unit::Unit;

pub trait Storage {
    fn get(&mut self, key: &str) -> Option<String>;
    fn set(&mut self, key: String, value: String);
    fn delete(&mut self, key: &str) -> bool;
    fn exists(&self, key: &str) -> bool;
    fn delete_multiple(&mut self, keys: Vec<String>) -> usize;
    fn exists_multiple(&self, keys: &[String]) -> usize;
    fn set_with_expiry(&mut self, key: String, value: String, expiry: u128);
    fn incr(&mut self, key: String) -> Option<i64>;
    fn config_get(&self, parameter: &str) -> Option<String>;
    fn get_keys(&self, pattern: &str) -> Vec<String>;
}

pub trait StorageList {
    fn rpush(&mut self, key: String, value: Vec<String>) -> usize;
    fn lrange(&self, key: &str, start: i64, end: i64) -> Option<Vec<String>>;
    fn lpush(&mut self, key: String, value: Vec<String>) -> usize;
    fn llen(&self, key: &str) -> usize;
    fn lpop(&mut self, key: &str, count: usize) -> Option<Vec<String>>;
    fn blpop(&mut self, keys: Vec<String>, token: mio::Token, timeout: u64) -> Option<Vec<String>>;
    fn brpop(&mut self, keys: Vec<String>, token: mio::Token, timeout: u64) -> Option<Vec<String>>;
}

pub trait StorageZSet {
    fn zadd(&mut self, key: String, score: f64, member: String) -> usize;
    fn zrank(&self, key: &str, member: &str) -> Option<usize>;
    fn zrange(&self, key: &str, start: i64, end: i64) -> Option<Vec<String>>;
    fn zcard(&self, key: &str) -> usize;
    fn zscore(&self, key: &str, member: &str) -> Option<f64>;
    fn zrem(&mut self, key: &str, member: &str) -> bool;
}

pub trait StorageStream {
    fn xadd(
        &mut self,
        key: String,
        id: String,
        fields: Vec<(String, String)>,
    ) -> Result<String, String>;
    fn xrange(
        &self,
        key: &str,
        start: String,
        end: String,
    ) -> Option<Vec<(String, Vec<(String, String)>)>>;
    fn xread(
        &mut self,
        token: mio::Token,
        block: Option<u64>,
        streams: Vec<(String, String)>,
    ) -> Option<Vec<(String, Vec<(String, Vec<(String, String)>)>)>>;
}

pub trait StorageGeo {
    fn geoadd(&mut self, key: String, longitude: f64, latitude: f64, member: String) -> Result<usize, String>;
    fn geopos(&self, key: &str, member: Vec<String>) -> Vec<Option<(f64, f64)>>;
    fn geodist(&self, key: &str, member1: &str, member2: &str) -> Option<f64>;
    fn geosearch(&self, key: &str, longitude: f64, latitude: f64, use_radius: bool, distance: f64, unit: String) -> Option<Vec<String>>;
}
