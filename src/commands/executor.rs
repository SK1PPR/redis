use super::{RedisCommand, RedisResponse};
use crate::server::event_loop_handle::EventLoopHandle;
use crate::storage::repl_config::ReplConfig;
use crate::storage::{MemoryStorage, Storage, StorageGeo, StorageList, StorageStream, StorageZSet};
use mio::Token;

pub trait CommandExecutor {
    fn execute(&mut self, command: RedisCommand, token: Token) -> RedisResponse;
}
pub struct RedisCommandExecutor {
    storage: MemoryStorage,
    handle: EventLoopHandle,
}

impl RedisCommandExecutor {
    pub fn new(handle: EventLoopHandle, repl_config: ReplConfig) -> Self {
        Self {
            storage: MemoryStorage::new(handle.clone(), repl_config),
            handle,
        }
    }

    pub fn new_with_file(
        handle: EventLoopHandle,
        directory: String,
        db_file_name: String,
        repl_config: ReplConfig,
    ) -> Self {
        let mut storage = MemoryStorage::new(handle.clone(), repl_config);
        storage.read_from_persistent_storage(&directory, &db_file_name);
        Self { storage, handle }
    }
}

pub trait Transactions {
    fn start_transaction(&mut self, token: mio::Token);
    fn exec_transaction(&mut self, token: mio::Token);
    fn discard_transaction(&mut self, token: mio::Token);
    fn is_transaction_command(&self, command: &RedisCommand) -> bool {
        matches!(
            command,
            RedisCommand::MULTI | RedisCommand::EXEC | RedisCommand::DISCARD
        )
    }
}

impl Transactions for RedisCommandExecutor {
    fn start_transaction(&mut self, token: mio::Token) {
        self.handle.start_multi(token);
    }

    fn exec_transaction(&mut self, token: mio::Token) {
        self.handle.execute_queue(token);
    }

    fn discard_transaction(&mut self, token: mio::Token) {
        self.handle.discard_queue(token);
    }
}

impl CommandExecutor for RedisCommandExecutor {
    fn execute(&mut self, command: RedisCommand, token: Token) -> RedisResponse {
        log::debug!("Executing command: {:?}", command);

        match command {
            RedisCommand::Ping(message) => match message {
                Some(msg) => RedisResponse::BulkString(Some(msg)),
                None => RedisResponse::pong(),
            },
            RedisCommand::Echo(message) => RedisResponse::BulkString(Some(message)),
            RedisCommand::Get(key) => match self.storage.get(&key) {
                Some(value) => RedisResponse::BulkString(Some(value)),
                None => RedisResponse::nil(),
            },
            RedisCommand::Set(key, value) => {
                self.storage.set(key, value);
                RedisResponse::ok()
            }
            RedisCommand::Del(keys) => {
                let deleted = self.storage.delete_multiple(keys);
                RedisResponse::Integer(deleted as i64)
            }
            RedisCommand::Exists(keys) => {
                let exists = self.storage.exists_multiple(&keys);
                RedisResponse::Integer(exists as i64)
            }
            RedisCommand::SetWithExpiry(key, value, expiry) => {
                self.storage.set_with_expiry(key, value, expiry);
                RedisResponse::ok()
            }
            RedisCommand::RPUSH(key, value) => {
                let length = self.storage.rpush(key, value);
                RedisResponse::Integer(length as i64)
            }
            RedisCommand::LPUSH(key, value) => {
                let length = self.storage.lpush(key, value);
                RedisResponse::Integer(length as i64)
            }
            RedisCommand::LLEN(key) => {
                let length = self.storage.llen(&key);
                RedisResponse::Integer(length as i64)
            }
            RedisCommand::LRANGE(key, start, end) => {
                let list = self.storage.lrange(&key, start, end);
                match list {
                    Some(items) => {
                        if items.is_empty() {
                            RedisResponse::Array(vec![])
                        } else {
                            RedisResponse::Array(
                                items
                                    .into_iter()
                                    .map(|item| RedisResponse::SimpleString(item))
                                    .collect(),
                            )
                        }
                    }
                    None => RedisResponse::Array(vec![]),
                }
            }
            RedisCommand::LPOP(key, count) => {
                let count = count.unwrap_or(1) as usize; // Default to 1 if not specified
                match self.storage.lpop(&key, count) {
                    Some(items) => {
                        if items.is_empty() {
                            RedisResponse::nil()
                        } else {
                            if count == 1 {
                                RedisResponse::BulkString(Some(items[0].clone()))
                            } else {
                                RedisResponse::Array(
                                    items
                                        .into_iter()
                                        .map(|item| RedisResponse::SimpleString(item))
                                        .collect(),
                                )
                            }
                        }
                    }
                    None => RedisResponse::nil(),
                }
            }
            RedisCommand::BLPOP(keys, timeout) => {
                let resp = self.storage.blpop(keys, token, timeout);
                if resp.is_some() {
                    return RedisResponse::BulkString(resp.unwrap().get(1).cloned());
                }
                return RedisResponse::Blocked;
            }
            RedisCommand::BRPOP(keys, timeout) => {
                let resp = self.storage.brpop(keys, token, timeout);
                if resp.is_some() {
                    return RedisResponse::BulkString(resp.unwrap().get(1).cloned());
                }
                return RedisResponse::Blocked;
            }
            RedisCommand::INCR(key) => match self.storage.incr(key) {
                Some(value) => RedisResponse::Integer(value),
                None => RedisResponse::error("value is not an integer or out of range"),
            },
            RedisCommand::MULTI => {
                self.start_transaction(token);
                RedisResponse::Empty
            }
            RedisCommand::EXEC => {
                self.exec_transaction(token);
                RedisResponse::Empty
            }
            RedisCommand::DISCARD => {
                self.discard_transaction(token);
                RedisResponse::Empty
            }
            RedisCommand::ZADD(key, score, member) => {
                let added = self.storage.zadd(key, score, member);
                RedisResponse::Integer(added as i64)
            }
            RedisCommand::ZRANK(key, member) => match self.storage.zrank(&key, &member) {
                Some(rank) => RedisResponse::Integer(rank as i64),
                None => RedisResponse::nil(),
            },
            RedisCommand::ZRANGE(key, start, end) => match self.storage.zrange(&key, start, end) {
                Some(members) => {
                    if members.is_empty() {
                        RedisResponse::Array(vec![])
                    } else {
                        RedisResponse::Array(
                            members
                                .into_iter()
                                .map(|member| RedisResponse::SimpleString(member))
                                .collect(),
                        )
                    }
                }
                None => RedisResponse::Array(vec![]),
            },
            RedisCommand::ZCARD(key) => {
                let count = self.storage.zcard(&key);
                RedisResponse::Integer(count as i64)
            }
            RedisCommand::ZSCORE(key, member) => match self.storage.zscore(&key, &member) {
                Some(score) => RedisResponse::BulkString(Some(score.to_string())),
                None => RedisResponse::nil(),
            },
            RedisCommand::ZREM(key, member) => {
                let removed = self.storage.zrem(&key, &member);
                if removed {
                    RedisResponse::Integer(1)
                } else {
                    RedisResponse::Integer(0)
                }
            }
            RedisCommand::TYPE(key) => RedisResponse::SimpleString(self.storage.get_type(&key)),
            RedisCommand::XADD(key, id, fields) => {
                match self.storage.xadd(key, id.unwrap(), fields) {
                    Ok(entry_id) => RedisResponse::BulkString(Some(entry_id)),
                    Err(err_msg) => {
                        log::debug!("XADD error: {}", err_msg);
                        RedisResponse::error(&err_msg)
                    }
                }
            }
            RedisCommand::XRANGE(key, start, end) => match self.storage.xrange(&key, start, end) {
                Some(entries) => {
                    if entries.is_empty() {
                        RedisResponse::Array(vec![])
                    } else {
                        RedisResponse::Array(
                            entries
                                .into_iter()
                                .map(|(id, fields)| {
                                    let mut field_array = Vec::new();
                                    for (field, value) in fields {
                                        field_array.push(RedisResponse::BulkString(Some(field)));
                                        field_array.push(RedisResponse::BulkString(Some(value)));
                                    }
                                    RedisResponse::Array(vec![
                                        RedisResponse::BulkString(Some(id)),
                                        RedisResponse::Array(field_array),
                                    ])
                                })
                                .collect(),
                        )
                    }
                }
                None => RedisResponse::Array(vec![]),
            },
            RedisCommand::XREAD(block, streams) => {
                match self.storage.xread(token, block, streams) {
                    Some(results) => {
                        if results.is_empty() {
                            RedisResponse::Array(vec![])
                        } else {
                            RedisResponse::Array(
                                results
                                    .into_iter()
                                    .map(|(key, entries)| {
                                        let entry_array: Vec<RedisResponse> = entries
                                            .into_iter()
                                            .map(|(id, fields)| {
                                                let mut field_array = Vec::new();
                                                for (field, value) in fields {
                                                    field_array.push(RedisResponse::BulkString(
                                                        Some(field),
                                                    ));
                                                    field_array.push(RedisResponse::BulkString(
                                                        Some(value),
                                                    ));
                                                }
                                                RedisResponse::Array(vec![
                                                    RedisResponse::BulkString(Some(id)),
                                                    RedisResponse::Array(field_array),
                                                ])
                                            })
                                            .collect();
                                        RedisResponse::Array(vec![
                                            RedisResponse::BulkString(Some(key)),
                                            RedisResponse::Array(entry_array),
                                        ])
                                    })
                                    .collect(),
                            )
                        }
                    }
                    None => RedisResponse::Empty,
                }
            }
            RedisCommand::GEOADD(key, longitude, latitude, member) => {
                match self.storage.geoadd(key, longitude, latitude, member) {
                    Ok(added) => RedisResponse::Integer(added as i64),
                    Err(err_msg) => {
                        log::debug!("GEOADD error: {}", err_msg);
                        RedisResponse::error(&err_msg)
                    }
                }
            }
            RedisCommand::GEOPOS(key, member) => {
                let positions = self.storage.geopos(&key, member);
                let response_array: Vec<RedisResponse> = positions
                    .into_iter()
                    .map(|pos_opt| match pos_opt {
                        Some((lon, lat)) => RedisResponse::Array(vec![
                            RedisResponse::BulkString(Some(lon.to_string())),
                            RedisResponse::BulkString(Some(lat.to_string())),
                        ]),
                        None => RedisResponse::NullArray,
                    })
                    .collect();
                RedisResponse::Array(response_array)
            }
            RedisCommand::GEODIST(key, from, to) => match self.storage.geodist(&key, &from, &to) {
                Some(distance) => RedisResponse::BulkString(Some(format!("{:.5}", distance))),
                None => RedisResponse::nil(),
            },
            RedisCommand::CONFIG(subcommand, parameter) => {
                match subcommand.to_uppercase().as_str() {
                    "GET" => match self.storage.config_get(&parameter) {
                        Some(value) => RedisResponse::Array(vec![
                            RedisResponse::BulkString(Some(parameter)),
                            RedisResponse::BulkString(Some(value)),
                        ]),
                        None => RedisResponse::Array(vec![]),
                    },
                    _ => RedisResponse::error("Unsupported CONFIG subcommand"),
                }
            }
            RedisCommand::KEYS(pattern) => {
                let keys = self.storage.get_keys(&pattern);
                if keys.is_empty() {
                    RedisResponse::Array(vec![])
                } else {
                    RedisResponse::Array(
                        keys.into_iter()
                            .map(|key| RedisResponse::BulkString(Some(key)))
                            .collect(),
                    )
                }
            }
        }
    }
}
