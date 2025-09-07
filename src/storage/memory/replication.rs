use super::{MemoryStorage, Replication};
use crate::commands::{RedisCommand, RedisResponse};
use crate::storage::file_utils::FileUtils;

impl Replication for MemoryStorage {
    fn add_replication_client(&mut self, token: mio::Token) {
        self.replication_clients.insert(token);
    }

    fn send_file(&self, token: mio::Token) {
        if self.replication_clients.contains(&token) {
            let contents = FileUtils::get_db_as_file();
            self.handle.send_file(token, contents);
        }
    }

    fn replicate_command(&self, command: crate::RedisCommand) {
        let resp = command_to_response(command).unwrap();
        for &token in &self.replication_clients {
            self.handle.send_command(token, resp.clone());
        }
    }
}

fn command_to_response(command: RedisCommand) -> Option<RedisResponse> {
    match command {
        RedisCommand::Set(key, value) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("SET".to_string()),
            RedisResponse::BulkString(Some(key)),
            RedisResponse::BulkString(Some(value)),
        ])),
        RedisCommand::SetWithExpiry(key, value, expiry) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("SET".to_string()),
            RedisResponse::BulkString(Some(key)),
            RedisResponse::BulkString(Some(value)),
            RedisResponse::SimpleString("PX".to_string()),
            RedisResponse::SimpleString(expiry.to_string()),
        ])),
        RedisCommand::Del(keys) => Some(RedisResponse::Array(
            std::iter::once(RedisResponse::SimpleString("DEL".to_string()))
                .chain(keys.into_iter().map(|k| RedisResponse::BulkString(Some(k))))
                .collect(),
        )),
        RedisCommand::RPUSH(key, values) => Some(RedisResponse::Array(
            std::iter::once(RedisResponse::SimpleString("RPUSH".to_string()))
                .chain(std::iter::once(RedisResponse::BulkString(Some(key))))
                .chain(
                    values
                        .into_iter()
                        .map(|v| RedisResponse::BulkString(Some(v))),
                )
                .collect(),
        )),
        RedisCommand::LPUSH(key, values) => Some(RedisResponse::Array(
            std::iter::once(RedisResponse::SimpleString("LPUSH".to_string()))
                .chain(std::iter::once(RedisResponse::BulkString(Some(key))))
                .chain(
                    values
                        .into_iter()
                        .map(|v| RedisResponse::BulkString(Some(v))),
                )
                .collect(),
        )),
        RedisCommand::INCR(key) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("INCR".to_string()),
            RedisResponse::BulkString(Some(key)),
        ])),
        RedisCommand::ZADD(key, score, value) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("ZADD".to_string()),
            RedisResponse::BulkString(Some(key)),
            RedisResponse::SimpleString(score.to_string()),
            RedisResponse::BulkString(Some(value)),
        ])),
        RedisCommand::ZREM(key, value) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("ZREM".to_string()),
            RedisResponse::BulkString(Some(key)),
            RedisResponse::BulkString(Some(value)),
        ])),
        RedisCommand::XADD(key, id, entries) => {
            let mut array = vec![
                RedisResponse::SimpleString("XADD".to_string()),
                RedisResponse::BulkString(Some(key)),
            ];

            if let Some(stream_id) = id {
                array.push(RedisResponse::BulkString(Some(stream_id)));
            } else {
                array.push(RedisResponse::SimpleString("*".to_string()));
            }

            for (field, value) in entries {
                array.push(RedisResponse::BulkString(Some(field)));
                array.push(RedisResponse::BulkString(Some(value)));
            }

            Some(RedisResponse::Array(array))
        }
        RedisCommand::GEOADD(key, longitude, latitude, member) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("GEOADD".to_string()),
            RedisResponse::BulkString(Some(key)),
            RedisResponse::SimpleString(longitude.to_string()),
            RedisResponse::SimpleString(latitude.to_string()),
            RedisResponse::BulkString(Some(member)),
        ])),
        RedisCommand::PUBLISH(channel, message) => Some(RedisResponse::Array(vec![
            RedisResponse::SimpleString("PUBLISH".to_string()),
            RedisResponse::BulkString(Some(channel)),
            RedisResponse::BulkString(Some(message)),
        ])),
        _ => None,
    }
}
