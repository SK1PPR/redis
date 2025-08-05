use super::client::Client;
use crate::commands::{CommandExecutor, CommandParser, RedisCommandExecutor};
use crate::protocol::RespParser;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io;

const SERVER_TOKEN: Token = Token(0);

pub struct EventLoop {
    poll: Poll,
    events: Events,
    server: TcpListener,
    clients: HashMap<Token, Client>,
    command_executor: RedisCommandExecutor,
    next_token: usize,
}

impl EventLoop {
    pub fn new(mut server: TcpListener) -> io::Result<Self> {
        let poll = Poll::new()?;
        let events = Events::with_capacity(128);

        // Register server socket for accept events
        poll.registry()
            .register(&mut server, SERVER_TOKEN, Interest::READABLE)?;

        Ok(EventLoop {
            poll,
            events,
            server,
            clients: HashMap::new(),
            command_executor: RedisCommandExecutor::new(),
            next_token: 1, // 0 is reserved for server
        })
    }

    pub fn run(&mut self) -> io::Result<()> {
        log::info!("Event loop started");

        loop {
            // Block until events are ready
            self.poll.poll(&mut self.events, None)?;

            // Collect events to avoid borrowing conflicts
            let events_to_process: Vec<_> = self
                .events
                .iter()
                .map(|event| {
                    (
                        event.token(),
                        event.is_readable(),
                        event.is_writable(),
                        event.is_read_closed(),
                        event.is_write_closed(),
                    )
                })
                .collect();

            for (token, is_readable, is_writable, is_read_closed, is_write_closed) in
                events_to_process
            {
                match token {
                    SERVER_TOKEN => {
                        loop {
                            match self.server.accept() {
                                Ok((socket, addr)) => {
                                    let token = Token(self.next_token);
                                    self.next_token += 1;

                                    log::info!(
                                        "New client connection from {} with token {}",
                                        addr,
                                        token.0
                                    );

                                    let mut socket = socket;

                                    // Register new client for read events
                                    self.poll.registry().register(
                                        &mut socket,
                                        token,
                                        Interest::READABLE,
                                    )?;

                                    let client = Client::new(socket, token);
                                    self.clients.insert(token, client);
                                }
                                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    // No more connections to accept right now
                                    break;
                                }
                                Err(e) => {
                                    log::error!("Error accepting connection: {}", e);
                                    return Err(e);
                                }
                            }
                        }
                    }
                    token => {
                        if !self.clients.contains_key(&token) {
                            return Ok(());
                        }

                        let mut should_close = false;

                        if is_readable {
                            match self.handle_client_read(token) {
                                Ok(_) => {}
                                Err(e) => {
                                    log::error!(
                                        "Error handling read for client {}: {}",
                                        token.0,
                                        e
                                    );
                                    should_close = true;
                                }
                            }
                        }

                        if is_writable && !should_close {
                            match self.handle_client_write(token) {
                                Ok(_) => {}
                                Err(e) => {
                                    log::error!(
                                        "Error handling write for client {}: {}",
                                        token.0,
                                        e
                                    );
                                    should_close = true;
                                }
                            }
                        }

                        if is_read_closed || is_write_closed {
                            should_close = true;
                        }

                        if should_close {
                            self.close_client(token)?;
                        }
                    }
                }
            }

            // Clean up closed connections
            self.cleanup_closed_clients()?;
        }
    }

    fn handle_client_read(&mut self, token: Token) -> io::Result<()> {
        // Read data from client
        let bytes_read = {
            let client = self.clients.get_mut(&token).unwrap();
            client.read_data()?
        };

        if bytes_read == 0 {
            return Ok(());
        }

        // Process any complete commands
        self.process_client_commands(token)?;

        Ok(())
    }

    fn handle_client_write(&mut self, token: Token) -> io::Result<()> {
        let should_switch_to_read = {
            let client = self.clients.get_mut(&token).unwrap();
            client.write_data()?;
            !client.has_pending_writes()
        };

        if should_switch_to_read {
            // Switch back to read mode
            let client = self.clients.get_mut(&token).unwrap();
            self.poll
                .registry()
                .reregister(&mut client.socket, token, Interest::READABLE)?;
        }

        Ok(())
    }

    fn process_client_commands(&mut self, token: Token) -> io::Result<()> {
        let commands_and_consumed = {
            let client = self.clients.get(&token).unwrap();
            RespParser::parse_commands(&client.read_buffer)
        };

        let (commands, bytes_consumed) = commands_and_consumed;

        if bytes_consumed > 0 {
            // Remove processed bytes from buffer
            let client = self.clients.get_mut(&token).unwrap();
            client.extract_read_data(bytes_consumed);
        }

        // Process each command
        for command_args in commands {
            if command_args.is_empty() {
                continue;
            }

            log::debug!(
                "Processing command from client {}: {:?}",
                token.0,
                command_args
            );

            let response = match CommandParser::parse(command_args) {
                Ok(command) => self.command_executor.execute(command),
                Err(error) => crate::commands::RedisResponse::error(&error),
            };

            // Add response to client's write buffer
            let client = self.clients.get_mut(&token).unwrap();
            client.add_response(response.to_resp());

            // Switch to write mode if we have data to send
            if client.has_pending_writes() {
                self.poll
                    .registry()
                    .reregister(&mut client.socket, token, Interest::WRITABLE)?;
            }
        }

        Ok(())
    }

    fn close_client(&mut self, token: Token) -> io::Result<()> {
        if let Some(mut client) = self.clients.remove(&token) {
            log::info!("Closing client connection {}", token.0);
            let _ = self.poll.registry().deregister(&mut client.socket);
        }
        Ok(())
    }

    fn cleanup_closed_clients(&mut self) -> io::Result<()> {
        let closed_tokens: Vec<Token> = self
            .clients
            .iter()
            .filter(|(_, client)| client.is_closed())
            .map(|(token, _)| *token)
            .collect();

        for token in closed_tokens {
            self.close_client(token)?;
        }

        Ok(())
    }
}
