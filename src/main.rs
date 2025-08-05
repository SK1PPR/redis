#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Read, Write};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut buf = [0; 1024];
                while stream.peek(buf.as_mut()).unwrap() > 0 {
                    let bytes_read = stream.read(&mut buf).unwrap();
                    if bytes_read == 0 {
                        break;
                    }
                    
                    // Process the command (for simplicity, we just echo it back)
                    println!("Received: {}", String::from_utf8_lossy(&buf[..bytes_read]));

                    // Break the bytes_read into multiple PING commands and send PONG responses for each
                    buf.chunks_mut(5).for_each(|chunk| {
                        if chunk.starts_with(b"PING") {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                    });
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
