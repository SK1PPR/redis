#![allow(unused_imports)]
use codecrafters_redis::server::RedisServer;
use std::io;

fn main() -> io::Result<()> {
    env_logger::init();
    
    let addr = "127.0.0.1:6379";
    println!("Starting Redis server on {}", addr);
    
    let mut server = RedisServer::new(addr)?;
    server.run()
}
