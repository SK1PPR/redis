use mio::{net::TcpStream, Token};
use std::io::{self, Read, Write, ErrorKind};

#[derive(Debug, PartialEq, Eq)]
pub enum ClientState {
    Reading,
    Writing,
    BlockedOnBlpop(Vec<String>),
    BlockedOnBrpop(Vec<String>),
    Closed,
}

pub struct Client {
    pub socket: TcpStream,
    pub token: Token,
    pub read_buffer: Vec<u8>,
    pub write_buffer: Vec<u8>,
    pub write_pos: usize,
    pub state: ClientState,
}

impl Client {
    pub fn new(socket: TcpStream, token: Token) -> Self {
        Self {
            socket,
            token,
            read_buffer: Vec::with_capacity(4096),
            write_buffer: Vec::new(),
            write_pos: 0,
            state: ClientState::Reading,
        }
    }
    
    pub fn read_data(&mut self) -> io::Result<usize> {

        if self.state != ClientState::Reading {
            return Ok(0);
        }

        let mut temp_buffer = [0u8; 4096];
        let mut total_read = 0;
        
        loop {
            match self.socket.read(&mut temp_buffer) {
                Ok(0) => {
                    // Connection closed by client
                    log::debug!("Client {} closed connection", self.token.0);
                    self.state = ClientState::Closed;
                    return Ok(total_read);
                }
                Ok(n) => {
                    log::debug!("Read {} bytes from client {}", n, self.token.0);
                    self.read_buffer.extend_from_slice(&temp_buffer[..n]);
                    total_read += n;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // No more data available right now
                    break;
                }
                Err(e) => {
                    log::error!("Error reading from client {}: {}", self.token.0, e);
                    self.state = ClientState::Closed;
                    return Err(e);
                }
            }
        }
        
        Ok(total_read)
    }
    
    pub fn write_data(&mut self) -> io::Result<usize> {
        if self.write_pos >= self.write_buffer.len() {
            return Ok(0);
        }
        
        let mut total_written = 0;
        
        loop {
            match self.socket.write(&self.write_buffer[self.write_pos..]) {
                Ok(0) => {
                    // Socket closed
                    self.state = ClientState::Closed;
                    break;
                }
                Ok(n) => {
                    log::debug!("Wrote {} bytes to client {}", n, self.token.0);
                    self.write_pos += n;
                    total_written += n;
                    
                    if self.write_pos >= self.write_buffer.len() {
                        // Finished writing all data
                        self.clear_write_buffer();
                        self.state = ClientState::Reading;
                        break;
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // Socket not ready for more writing
                    break;
                }
                Err(e) => {
                    log::error!("Error writing to client {}: {}", self.token.0, e);
                    self.state = ClientState::Closed;
                    return Err(e);
                }
            }
        }
        
        Ok(total_written)
    }
    
    pub fn add_response(&mut self, response: String) {
        log::debug!("Adding response to client {}: {}", self.token.0, response.trim());
        self.write_buffer.extend_from_slice(response.as_bytes());
        self.state = ClientState::Writing;
    }
    
    pub fn has_pending_writes(&self) -> bool {
        self.write_pos < self.write_buffer.len()
    }
    
    pub fn clear_write_buffer(&mut self) {
        self.write_buffer.clear();
        self.write_pos = 0;
    }
    
    pub fn is_closed(&self) -> bool {
        matches!(self.state, ClientState::Closed)
    }
    
    pub fn extract_read_data(&mut self, bytes: usize) -> Vec<u8> {
        let data = self.read_buffer.drain(..bytes).collect();
        data
    }

    pub fn set_blocked_on_blpop(&mut self, keys: Vec<String>) {
        self.state = ClientState::BlockedOnBlpop(keys);
    }

    pub fn set_blocked_on_brpop(&mut self, keys: Vec<String>) {
        self.state = ClientState::BlockedOnBrpop(keys);
    }

    pub fn unblock(&mut self) {
        self.state = ClientState::Reading;
    }

    pub fn is_blocked(&self) -> bool {
        matches!(self.state, ClientState::BlockedOnBlpop(_) | ClientState::BlockedOnBrpop(_))
    }

    pub fn get_blocked_keys(&self) -> Option<&Vec<String>> {
        match &self.state {
            ClientState::BlockedOnBlpop(keys) | ClientState::BlockedOnBrpop(keys) => Some(keys),
            _ => None,
        }
    }
}