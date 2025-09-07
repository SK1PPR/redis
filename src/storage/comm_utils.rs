use super::repl_config::ReplConfig;
use mio::net::TcpStream;

use std::io::{self, Write};
use std::io::{Read,};
use std::time::{Duration, Instant};
pub struct CommunicationUtils;

impl CommunicationUtils {
    fn initial_master_connection(repl_config: &ReplConfig) -> io::Result<Option<TcpStream>> {
        if let ReplConfig::Slave(slave_cfg) = repl_config {
            let master_addr = format!("{}:{}", slave_cfg.master_host, slave_cfg.master_port);
            println!("Attempting to connect to master at {}", master_addr);
            match TcpStream::connect(master_addr.parse().unwrap()) {
                Ok(stream) => {
                    log::info!("Connected to master at {}", master_addr);
                    Ok(Some(stream))
                }
                Err(e) => {
                    log::error!("Failed to connect to master at {}: {}", master_addr, e);
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    fn send_ping(stream: &mut TcpStream) -> io::Result<()> {
        let ping_command = b"*1\r\n$4\r\nPING\r\n";
        stream.write_all(ping_command)?;
        stream.flush()?;
        Ok(())
    }

    fn send_replconf(stream: &mut TcpStream) -> io::Result<()> {
        let replconf_command =
            format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n");
        stream.write_all(replconf_command.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    fn send_replconf_capabilities(stream: &mut TcpStream) -> io::Result<()> {
        let replconf_command = format!("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        stream.write_all(replconf_command.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    fn send_psync(stream: &mut TcpStream) -> io::Result<()> {
        let psync_command = format!("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
        stream.write_all(psync_command.as_bytes())?;
        stream.flush()?;
        Ok(())
    }

    pub fn setup_replication(repl_config: &ReplConfig) -> io::Result<Option<TcpStream>> {
        if let Some(mut stream) = Self::initial_master_connection(repl_config)? {
            
            // Send initial PING to master
            Self::send_ping(&mut stream)?;
            
            // Handle non-blocking reads for ping response
            let mut buffer = [0; 1024];
            let mut response = Self::read_nonblocking(&mut stream, &mut buffer)?;
            log::info!("Received ping response: {}", String::from_utf8_lossy(&response));

            // Send REPLCONF listening-port
            Self::send_replconf(&mut stream)?;
            response = Self::read_nonblocking(&mut stream, &mut buffer)?;
            log::info!("Received replconf response: {}", String::from_utf8_lossy(&response));

            // Send REPLCONF capabilities
            Self::send_replconf_capabilities(&mut stream)?;
            response = Self::read_nonblocking(&mut stream, &mut buffer)?;
            log::info!("Received capabilities response: {}", String::from_utf8_lossy(&response));

            // Send PSYNC
            Self::send_psync(&mut stream)?;
            response = Self::read_nonblocking(&mut stream, &mut buffer)?;
            log::info!("Received psync response: {}", String::from_utf8_lossy(&response));
            
            Ok(Some(stream))
        } else {
            Ok(None)
        }
    }

    // Helper method for non-blocking reads with timeout
    fn read_nonblocking<'a>(stream: &mut TcpStream, buffer: &'a mut [u8]) -> io::Result<&'a [u8]> {
        
        let timeout = Duration::from_secs(5);
        let start = Instant::now();
        let mut bytes_read = 0;
        
        loop {
            match stream.read(&mut buffer[bytes_read..]) {
                Ok(0) => {
                    if bytes_read == 0 {
                        return Err(io::Error::new(io::ErrorKind::ConnectionReset, "Connection closed"));
                    } else {
                        break;
                    }
                },
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == buffer.len() || buffer[..bytes_read].contains(&b'\n') {
                        break;
                    }
                },
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if start.elapsed() > timeout {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "Read timed out"));
                    }
                    // Small sleep to avoid burning CPU in tight loop
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                },
                Err(e) => return Err(e),
            }
        }
        
        Ok(&buffer[..bytes_read])
    }
}
