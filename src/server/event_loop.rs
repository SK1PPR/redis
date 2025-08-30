use super::client::Client;
use super::event_loop_handle::{EventLoopHandle, EventLoopMessage};
use crate::commands::executor::Transactions;
use crate::commands::{CommandExecutor, CommandParser, RedisCommandExecutor};
use crate::protocol::RespParser;
use crate::RedisResponse;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token, Waker};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

const SERVER_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(usize::MAX);

pub struct EventLoop {
    poll: Poll,
    events: Events,
    server: TcpListener,
    clients: HashMap<Token, Client>,
    command_executor: RedisCommandExecutor,
    next_token: usize,

    // Communication with other modules
    #[allow(dead_code)] // Never actually read but given to other modules
    waker: std::sync::Arc<Waker>,

    message_receiver: Receiver<EventLoopMessage>,
    event_loop_handle: EventLoopHandle,

    // Blocking operation tracking
    blocked_clients_timeout: HashMap<Token, Instant>,

    // Multi operation tracking
    multi_clients: HashSet<Token>,
}

impl EventLoop {
    pub fn new(mut server: TcpListener) -> io::Result<Self> {
        let poll = Poll::new()?;
        let events = Events::with_capacity(128);
        let waker = std::sync::Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

        // Create communication channel
        let (sender, receiver) = mpsc::channel();
        let handle = EventLoopHandle::new(sender, std::sync::Arc::clone(&waker));
        // Register server socket for accept events
        poll.registry()
            .register(&mut server, SERVER_TOKEN, Interest::READABLE)?;

        Ok(EventLoop {
            poll,
            events,
            server,
            clients: HashMap::new(),
            command_executor: RedisCommandExecutor::new(handle.clone()),
            next_token: 1, // 0 is reserved for server
            waker,
            message_receiver: receiver,
            event_loop_handle: handle,
            blocked_clients_timeout: HashMap::new(),
            multi_clients: HashSet::new(),
        })
    }

    pub fn get_handle(&self) -> EventLoopHandle {
        self.event_loop_handle.clone()
    }

    pub fn run(&mut self) -> io::Result<()> {
        log::info!("Event loop started");

        loop {
            // Calculate timeout for blocked clients
            let timeout = self.calculate_poll_timeout();

            // Block until events are ready or timeout
            self.poll.poll(&mut self.events, timeout)?;

            // Check for timed out blocked clients first
            self.handle_blocked_client_timeouts()?;

            // Process messages from other modules
            self.process_messages()?;

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
                        self.handle_new_connections()?;
                    }
                    WAKER_TOKEN => {
                        // Just wake up, no action needed
                        continue;
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

    fn handle_new_connections(&mut self) -> io::Result<()> {
        loop {
            match self.server.accept() {
                Ok((socket, addr)) => {
                    let token = Token(self.next_token);
                    self.next_token += 1;

                    log::info!("New client connection from {} with token {}", addr, token.0);

                    let mut socket = socket;

                    // Register new client for read events
                    self.poll
                        .registry()
                        .register(&mut socket, token, Interest::READABLE)?;

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

        Ok(())
    }

    fn process_messages(&mut self) -> io::Result<()> {
        while let Ok(message) = self.message_receiver.try_recv() {
            match message {
                EventLoopMessage::UnblockClient { token, response } => {
                    self.unblock_client_internal(token, response)?;
                }
                EventLoopMessage::BlockClient { token, timeout } => {
                    self.block_client_internal(token, timeout)?;
                }
                EventLoopMessage::StartMulti { token } => {
                    self.start_multi_internal(token)?;
                }
                EventLoopMessage::ExecuteQueue { token } => {
                    self.exec_queue_internal(token)?;
                }
                EventLoopMessage::DiscardQueue { token } => {
                    self.discard_queue_internal(token)?;
                }
            }
        }

        Ok(())
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

            let client = self.clients.get_mut(&token).unwrap();
            let response = match CommandParser::parse(command_args) {
                Ok(command) => {
                    if self.multi_clients.contains(&token)
                        && !RedisCommandExecutor::is_transaction_command(
                            &self.command_executor,
                            &command,
                        )
                    {
                        client.execution_queue.push(command);
                        RedisResponse::queued()
                    } else {
                        self.command_executor.execute(command, token)
                    }
                }
                Err(error) => crate::commands::RedisResponse::error(&error),
            };

            // For blocking commands, the executor will handle the blocking via the handle
            // We only send responses for non-blocking commands here
            if !matches!(response, RedisResponse::Blocked) {
                client.add_response(response.to_resp());

                // Switch to write mode if we have data to send
                if client.has_pending_writes() {
                    self.poll.registry().reregister(
                        &mut client.socket,
                        token,
                        Interest::WRITABLE,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn write_response(&mut self, token: Token, response: RedisResponse) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(&token) {
            client.add_response(response.to_resp());

            if client.has_pending_writes() {
                self.poll
                    .registry()
                    .reregister(&mut client.socket, token, Interest::WRITABLE)?;
            }
        }

        Ok(())
    }

    fn block_client_internal(&mut self, token: Token, timeout_milliseconds: u64) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(&token) {
            client.block();

            // Set timeout if specified
            if timeout_milliseconds != 0 {
                let timeout_instant = Instant::now() + Duration::from_millis(timeout_milliseconds);
                self.blocked_clients_timeout.insert(token, timeout_instant);
            }

            log::debug!(
                "Blocking client {} for {} seconds",
                token.0,
                timeout_milliseconds
            );
        }

        Ok(())
    }

    fn unblock_client_internal(&mut self, token: Token, response: RedisResponse) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(&token) {
            if client.is_blocked() {
                client.unblock();
                self.blocked_clients_timeout.remove(&token);
                return self.write_response(token, response);
            }
        }

        Ok(())
    }

    fn start_multi_internal(&mut self, token: Token) -> io::Result<()> {
        if let Some(_) = self.clients.get_mut(&token) {
            if self.multi_clients.contains(&token) {
                return self.write_response(
                    token,
                    RedisResponse::Error("MULTI calls can not be nested".to_string()),
                );
            }

            self.multi_clients.insert(token);
            return self.write_response(token, RedisResponse::ok());
        }

        Ok(())
    }

    fn discard_queue_internal(&mut self, token: Token) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(&token) {
            if !self.multi_clients.contains(&token) {
                return self.write_response(
                    token,
                    RedisResponse::Error("DISCARD without MULTI".to_string()),
                );
            }

            client.execution_queue.clear();
            self.multi_clients.remove(&token);
            self.write_response(token, RedisResponse::ok())?;
        }

        Ok(())
    }

    fn exec_queue_internal(&mut self, token: Token) -> io::Result<()> {
        if let Some(client) = self.clients.get_mut(&token) {
            if !self.multi_clients.contains(&token) {
                return self.write_response(
                    token,
                    RedisResponse::Error("EXEC without MULTI".to_string()),
                );
            }

            let mut responses = Vec::new();
            for command in client.execution_queue.drain(..) {
                let response = self.command_executor.execute(command, token);
                responses.push(response);
            }

            self.multi_clients.remove(&token);
            self.write_response(token, RedisResponse::Array(responses))?;
        }

        Ok(())
    }

    fn calculate_poll_timeout(&self) -> Option<Duration> {
        if self.blocked_clients_timeout.is_empty() {
            return None;
        }

        let now = Instant::now();
        let next_timeout = self.blocked_clients_timeout.values().min().copied()?;

        if next_timeout <= now {
            Some(Duration::from_millis(0))
        } else {
            Some(next_timeout - now)
        }
    }

    fn handle_blocked_client_timeouts(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let timed_out_clients: Vec<Token> = self
            .blocked_clients_timeout
            .iter()
            .filter(|(_, &timeout)| timeout <= now)
            .map(|(&token, _)| token)
            .collect();

        for token in timed_out_clients {
            self.blocked_clients_timeout.remove(&token);

            if let Some(client) = self.clients.get_mut(&token) {
                if client.is_blocked() {
                    log::debug!("Client {} has timed out", token.0);
                    self.unblock_client_internal(token, RedisResponse::null_array())?;
                }
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
