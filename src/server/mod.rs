pub mod client;
pub mod event_loop;
pub mod event_loop_handle;

use event_loop::EventLoop;
use mio::net::TcpListener;
use std::io;

pub struct RedisServer {
    event_loop: EventLoop,
}

impl RedisServer {
    pub fn new(
        addr: &str,
        directory: Option<String>,
        db_file_name: Option<String>,
    ) -> io::Result<Self> {
        let address = addr
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid address"))?;

        let listener = TcpListener::bind(address)?;
        log::info!("Redis server listening on {}", addr);

        let event_loop = if let (Some(dir), Some(dbfilename)) = (directory, db_file_name) {
            EventLoop::new_with_file(listener, dir, dbfilename)?
        } else {
            EventLoop::new(listener)?
        };

        Ok(Self { event_loop })
    }

    pub fn run(&mut self) -> io::Result<()> {
        log::info!("Starting Redis server event loop");
        self.event_loop.run()
    }
}
