#![allow(unused_imports)]
use codecrafters_redis::server::RedisServer;
use std::io;

fn main() -> io::Result<()> {
    env_logger::init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut port = 6379;
    let mut dir = None;
    let mut dbfilename = None;

    for i in 0..args.len() {
        if args[i] == "--dir" && i + 1 < args.len() {
            dir = Some(args[i + 1].clone());
        } else if args[i] == "--dbfilename" && i + 1 < args.len() {
            dbfilename = Some(args[i + 1].clone());
        } else if args[i] == "--port" && i + 1 < args.len() {
            if let Ok(p) = args[i + 1].parse::<u16>() {
                port = p;
            }
        }
    }

    let addr = format!("127.0.0.1:{}", port);
    println!("Starting Redis server on {}", addr);

    let mut server = RedisServer::new(&addr, dir, dbfilename)?;
    server.run()
}
