pub mod memory;

pub use memory::MemoryStorage;

pub trait Storage {
    fn get(&mut self, key: &str) -> Option<String>;
    fn set(&mut self, key: String, value: String);
    fn delete(&mut self, key: &str) -> bool;
    fn exists(&self, key: &str) -> bool;
    fn delete_multiple(&mut self, keys: Vec<String>) -> usize;
    fn exists_multiple(&self, keys: &[String]) -> usize;
    fn set_with_expiry(&mut self, key: String, value: String, expiry: u128);
}

pub trait StorageList {
    fn rpush(&mut self, key: String, value: String) -> usize;
}