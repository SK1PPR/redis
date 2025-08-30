#![allow(unused_imports)]
use codecrafters_redis::server::RedisServer;
use std::io;

fn main() -> io::Result<()> {
    env_logger::init();

    let addr = "127.0.0.1:6379";
    println!("Starting Redis server on {}", addr);

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut dir = None;
    let mut dbfilename = None;

    for i in 0..args.len() {
        if args[i] == "--dir" && i + 1 < args.len() {
            dir = Some(args[i + 1].clone());
        } else if args[i] == "--dbfilename" && i + 1 < args.len() {
            dbfilename = Some(args[i + 1].clone());
        }
    }

    let mut server = RedisServer::new(addr, dir, dbfilename)?;
    server.run()
}
