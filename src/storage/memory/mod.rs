use mio::Token;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::commands::response::RedisResponse;
use crate::server::event_loop_handle::EventLoopHandle;
use crate::storage::file_utils::FileUtils;
use crate::storage::repl_config::ReplConfig;
use crate::storage::stream_member::StreamId;
use crate::storage::{
    Replication, Storage, StorageGeo, StorageList, StoragePubSub, StorageStream, StorageZSet, Unit,
};

mod replication;
mod storage;
mod storage_geo;
mod storage_list;
mod storage_pub_sub;
mod storage_stream;
mod storage_zset;

#[derive(Debug, Clone)]
enum BlockedType {
    List(bool),       // true for BLPOP, false for BRPOP
    Stream(StreamId), // ID to read after
}

#[derive(Debug, Clone)]
pub struct BlockedClient {
    token: Token,
    timeout: Option<Instant>,
    blocked_type: BlockedType,
}

impl BlockedClient {
    pub fn new_list(token: Token, timeout: Option<Instant>, left_blocked: bool) -> Self {
        Self {
            token,
            timeout,
            blocked_type: BlockedType::List(left_blocked),
        }
    }

    pub fn new_stream(token: Token, timeout: Option<Instant>, id: StreamId) -> Self {
        Self {
            token,
            timeout,
            blocked_type: BlockedType::Stream(id),
        }
    }

    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            Instant::now() >= timeout
        } else {
            false
        }
    }

    pub fn by_list(&self) -> bool {
        matches!(self.blocked_type, BlockedType::List(_))
    }

    pub fn by_stream(&self) -> bool {
        matches!(self.blocked_type, BlockedType::Stream(_))
    }

    pub fn left_blocked(&self) -> bool {
        if let BlockedType::List(left) = self.blocked_type {
            left
        } else {
            false
        }
    }

    pub fn stream_id(&self) -> Option<&StreamId> {
        if let BlockedType::Stream(ref id) = self.blocked_type {
            Some(id)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct MemoryStorage {
    storage: HashMap<String, Unit>,
    handle: EventLoopHandle,
    blocked_clients: HashMap<String, Vec<BlockedClient>>,
    dir: Option<String>,
    dbfilename: Option<String>,
    pub repl_config: ReplConfig,
    pubsub: HashMap<String, Vec<mio::Token>>, // channel -> subscribers
    replication_clients: HashSet<mio::Token>,
}

impl MemoryStorage {
    pub fn new(handle: EventLoopHandle, repl_config: ReplConfig) -> Self {
        Self {
            storage: HashMap::new(),
            handle,
            blocked_clients: HashMap::new(),
            dir: None,
            dbfilename: None,
            repl_config,
            pubsub: HashMap::new(),
            replication_clients: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn clear(&mut self) {
        self.storage.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.storage.keys()
    }

    // Helper method to unblock clients waiting on a specific key
    fn unblock_clients_for_key(&mut self, key: &str, blocked_on_list: bool) {
        if let Some(blocked_clients) = self.blocked_clients.remove(key) {
            log::debug!(
                "Unblocking {} clients waiting for key '{}'",
                blocked_clients.len(),
                key
            );

            for blocked_client in blocked_clients.clone() {
                if blocked_client.is_timed_out() {
                    log::debug!(
                        "Client with token {:?} has timed out while waiting for key '{}'",
                        blocked_client.token,
                        key
                    );
                    continue;
                }

                let response = if blocked_on_list {
                    self.response_blocked_on_list(blocked_client.clone(), key)
                } else {
                    self.response_blocked_on_stream(blocked_client.clone(), key)
                };

                if response.is_none() {
                    continue;
                }

                let response = response.unwrap();
                self.handle.unblock_client(blocked_client.token, response);

                // If we successfully unblocked a client, we might have consumed the only element
                // Check if there are more elements for remaining clients
                if let Some(list) = self
                    .storage
                    .get(key)
                    .and_then(|unit| unit.implementation.as_list())
                {
                    if list.is_empty() {
                        self.reblock_remaining_clients(key, blocked_clients, blocked_client.token);
                        break;
                    }
                } else {
                    self.reblock_remaining_clients(key, blocked_clients, blocked_client.token);
                    break;
                }
            }
        }
    }

    fn response_blocked_on_list(
        &mut self,
        blocked_client: BlockedClient,
        key: &str,
    ) -> Option<RedisResponse> {
        if !blocked_client.by_list() {
            return None;
        }

        if blocked_client.left_blocked() {
            // BLPOP - pop from left
            if let Some(mut popped) = self.lpop(key, 1) {
                if !popped.is_empty() {
                    Some(RedisResponse::Array(vec![
                        RedisResponse::BulkString(Some(key.to_string())),
                        RedisResponse::BulkString(Some(popped.remove(0))),
                    ]))
                } else {
                    None // No element available, keep client blocked
                }
            } else {
                None // Key doesn't exist or no elements
            }
        } else {
            // BRPOP - pop from right
            if let Some(list) = self
                .storage
                .get_mut(key)
                .and_then(|unit| unit.implementation.as_list_mut())
            {
                if !list.is_empty() {
                    let item = list.remove(list.len() - 1);
                    if list.is_empty() {
                        self.storage.remove(key);
                    }
                    Some(RedisResponse::Array(vec![
                        RedisResponse::BulkString(Some(key.to_string())),
                        RedisResponse::BulkString(Some(item)),
                    ]))
                } else {
                    None // No elements available
                }
            } else {
                None // Key doesn't exist
            }
        }
    }

    fn response_blocked_on_stream(
        &mut self,
        blocked_client: BlockedClient,
        key: &str,
    ) -> Option<RedisResponse> {
        if !blocked_client.by_stream() {
            return None;
        }

        let last_id = blocked_client.stream_id()?;

        if let Some(entries) = self.xrange(
            key,
            StreamId::next(last_id).to_string(),
            StreamId {
                timestamp: u64::MAX,
                sequence: u64::MAX,
            }
            .to_string(),
        ) {
            if !entries.is_empty() {
                let mut response_entries = Vec::new();
                for (entry_id, fields) in entries {
                    let mut field_responses = Vec::new();
                    for (field, value) in fields {
                        field_responses.push(RedisResponse::BulkString(Some(field)));
                        field_responses.push(RedisResponse::BulkString(Some(value)));
                    }
                    response_entries.push(RedisResponse::Array(vec![
                        RedisResponse::BulkString(Some(entry_id)),
                        RedisResponse::Array(field_responses),
                    ]));
                }
                return Some(RedisResponse::Array(vec![RedisResponse::Array(vec![
                    RedisResponse::BulkString(Some(key.to_string())),
                    RedisResponse::Array(response_entries),
                ])]));
            }
        }

        None // No new entries available
    }

    // Helper to re-block clients that couldn't be satisfied
    fn reblock_remaining_clients(
        &mut self,
        key: &str,
        mut all_clients: Vec<BlockedClient>,
        satisfied_token: Token,
    ) {
        all_clients.retain(|client| client.token.0 != satisfied_token.0);

        if !all_clients.is_empty() {
            self.blocked_clients.insert(key.to_string(), all_clients);
            log::debug!(
                "Re-blocked {} clients for key '{}'",
                self.blocked_clients.get(key).map_or(0, |v| v.len()),
                key
            );
        }
    }

    pub fn get_type(&self, key: &str) -> String {
        if let Some(unit) = self.storage.get(key) {
            if unit.is_expired() {
                log::debug!("Key '{}' has expired", key);
                return "none".to_string();
            }
            if unit.implementation.is_string() {
                "string".to_string()
            } else if unit.implementation.is_list() {
                "list".to_string()
            } else if unit.implementation.is_zset() {
                "zset".to_string()
            } else if unit.implementation.is_stream() {
                "stream".to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "none".to_string()
        }
    }

    pub fn read_from_persistent_storage(&mut self, dir: &str, dbfilename: &str) {
        self.dir = Some(dir.to_string());
        self.dbfilename = Some(dbfilename.to_string());

        if FileUtils::validate_db_file(dir, dbfilename) {
            log::info!(
                "Reading data from persistent storage at {}/{}",
                dir,
                dbfilename
            );
            match FileUtils::construct_db_from_file(dir, dbfilename) {
                Some(loaded_storage) => {
                    self.storage = loaded_storage;
                    log::info!(
                        "Successfully loaded {} keys from persistent storage",
                        self.storage.len()
                    );
                }
                None => {
                    println!("Error reading persistent storage");
                    log::error!("Error reading persistent storage");
                }
            }
            return;
        }
        println!("Persistent storage file not found or invalid. Starting with empty storage.");
        log::info!("Persistent storage file not found or invalid. Starting with empty storage.");
    }

    pub fn get_info_replication(&self) -> String {
        return self.repl_config.to_string();
    }

    pub fn add_subscriber(&mut self, token: mio::Token, channel: String) {
        self.pubsub
            .entry(channel)
            .or_insert_with(Vec::new)
            .push(token);
    }

    pub fn get_subscriptions(&self, token: mio::Token) -> Vec<String> {
        let mut channels = Vec::new();
        for (channel, subscribers) in &self.pubsub {
            if subscribers.contains(&token) {
                channels.push(channel.clone());
            }
        }
        channels
    }

    pub fn get_channel_subscriptions(&self, channel: &str) -> Vec<mio::Token> {
        self.pubsub.get(channel).cloned().unwrap_or_else(Vec::new)
    }

    pub fn remove_subscriber(&mut self, token: mio::Token, channel: String) -> usize {
        if let Some(subscribers) = self.pubsub.get_mut(&channel) {
            subscribers.retain(|&t| t != token);
            if subscribers.is_empty() {
                self.pubsub.remove(&channel);
                return 0;
            }
            return subscribers.len();
        }
        0
    }
}
