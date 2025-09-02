#![allow(unused_imports)]
use codecrafters_redis::{server::RedisServer, storage::repl_config::ReplConfig};
use std::io;

fn main() -> io::Result<()> {
    env_logger::init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let mut port = 6379;
    let mut dir = None;
    let mut dbfilename = None;

    let mut slave_of = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dir" if i + 1 < args.len() => {
                dir = Some(args[i + 1].clone());
                i += 2;
            }
            "--dbfilename" if i + 1 < args.len() => {
                dbfilename = Some(args[i + 1].clone());
                i += 2;
            }
            "--port" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse::<u16>() {
                    port = p;
                }
                i += 2;
            }
            "--replicaof" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse::<u16>() {
                    slave_of = Some(p);
                }
                i += 2;
            }
            _ => i += 1,
        }
    }

    let repl_config = if slave_of.is_some() {
        let master_port = slave_of.unwrap();
        ReplConfig::new_slave(
            "127.0.0.1".to_string(),
            port,
            "127.0.0.1".to_string(),
            master_port,
        )
    } else {
        ReplConfig::new_master("127.0.0.1".to_string(), port)
    };

    let mut server = RedisServer::new(dir, dbfilename, repl_config)?;
    server.run()
}
