pub mod event_loop;
pub mod client;

use event_loop::EventLoop;
use mio::net::TcpListener;
use std::io;

pub struct RedisServer {
    event_loop: EventLoop,
}

impl RedisServer {
    pub fn new(addr: &str) -> io::Result<Self> {
        let address = addr.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid address")
        })?;
        
        let listener = TcpListener::bind(address)?;
        log::info!("Redis server listening on {}", addr);
        
        let event_loop = EventLoop::new(listener)?;
        
        Ok(Self { event_loop })
    }
    
    pub fn run(&mut self) -> io::Result<()> {
        log::info!("Starting Redis server event loop");
        self.event_loop.run()
    }
}