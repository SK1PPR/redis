use super::RedisCommand;

pub struct CommandParser;

impl CommandParser {
    pub fn parse(args: Vec<String>) -> Result<RedisCommand, String> {
        if args.is_empty() {
            return Err("Empty command".to_string());
        }

        let command = args[0].to_uppercase();

        match command.as_str() {
            "PING" => Self::parse_ping(&args),
            "ECHO" => Self::parse_echo(&args),
            "GET" => Self::parse_get(&args),
            "SET" => Self::parse_set(&args),
            "DEL" => Self::parse_del(&args),
            "EXISTS" => Self::parse_exists(&args),
            "RPUSH" => Self::parse_rpush(&args),
            "LRANGE" => Self::parse_lrange(&args),
            "LPUSH" => Self::parse_lpush(&args),
            "LLEN" => Self::parse_llen(&args),
            "LPOP" => Self::parse_lpop(&args),
            "BLPOP" => Self::parse_blpop(&args),
            "BRPOP" => Self::parse_brpop(&args),
            "INCR" => Self::parse_incr(&args),
            "MULTI" => Self::parse_multi(&args),
            "EXEC" => Self::parse_exec(&args),
            "DISCARD" => Self::parse_discard(&args),
            "ZADD" => Self::parse_zadd(&args),
            "ZRANK" => Self::parse_zrank(&args),
            "ZRANGE" => Self::parse_zrange(&args),
            "ZCARD" => Self::parse_zcard(&args),
            "ZSCORE" => Self::parse_zscore(&args),
            "ZREM" => Self::parse_zrem(&args),
            "TYPE" => Self::parse_type(&args),
            "XADD" => Self::parse_xadd(&args),
            "XRANGE" => Self::parse_xrange(&args),
            "XREAD" => Self::parse_xread(&args),
            "GEOADD" => Self::parse_geoadd(&args),
            "GEOPOS" => Self::parse_geopos(&args),
            _ => Err(format!("Unknown command: {}", command)),
        }
    }

    fn parse_ping(args: &[String]) -> Result<RedisCommand, String> {
        match args.len() {
            1 => Ok(RedisCommand::Ping(None)),
            2 => Ok(RedisCommand::Ping(Some(args[1].clone()))),
            _ => Err("Wrong number of arguments for PING".to_string()),
        }
    }

    fn parse_echo(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for ECHO".to_string());
        }
        Ok(RedisCommand::Echo(args[1].clone()))
    }

    fn parse_get(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for GET".to_string());
        }
        Ok(RedisCommand::Get(args[1].clone()))
    }

    fn parse_set(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() == 3 {
            Ok(RedisCommand::Set(args[1].clone(), args[2].clone()))
        } else if args.len() == 5 {
            if args[3].to_ascii_uppercase() == "EX" {
                let expiry: u128 = args[4]
                    .parse()
                    .map_err(|_| "Invalid expiry value".to_string())?;
                Ok(RedisCommand::SetWithExpiry(
                    args[1].clone(),
                    args[2].clone(),
                    expiry * 1000,
                )) // Convert seconds to milliseconds
            } else if args[3].to_ascii_uppercase() == "PX" {
                let expiry: u128 = args[4]
                    .parse()
                    .map_err(|_| "Invalid expiry value".to_string())?;
                Ok(RedisCommand::SetWithExpiry(
                    args[1].clone(),
                    args[2].clone(),
                    expiry,
                ))
            } else {
                return Err("Invalid SET command format".to_string());
            }
        } else {
            return Err("Wrong number of arguments for SET".to_string());
        }
    }

    fn parse_del(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 2 {
            return Err("Wrong number of arguments for DEL".to_string());
        }
        Ok(RedisCommand::Del(args[1..].to_vec()))
    }

    fn parse_exists(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 2 {
            return Err("Wrong number of arguments for EXISTS".to_string());
        }
        Ok(RedisCommand::Exists(args[1..].to_vec()))
    }

    fn parse_rpush(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 3 {
            return Err("Wrong number of arguments for RPUSH".to_string());
        }
        Ok(RedisCommand::RPUSH(args[1].clone(), args[2..].to_vec()))
    }

    fn parse_lrange(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 4 {
            return Err("Wrong number of arguments for LRANGE".to_string());
        }
        let start: i64 = args[2]
            .parse()
            .map_err(|_| "Invalid start index".to_string())?;
        let end: i64 = args[3]
            .parse()
            .map_err(|_| "Invalid end index".to_string())?;
        Ok(RedisCommand::LRANGE(args[1].clone(), start, end))
    }

    fn parse_lpush(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 3 {
            return Err("Wrong number of arguments for LPUSH".to_string());
        }
        Ok(RedisCommand::LPUSH(args[1].clone(), args[2..].to_vec()))
    }

    fn parse_llen(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for LLEN".to_string());
        }
        Ok(RedisCommand::LLEN(args[1].clone()))
    }

    fn parse_lpop(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 2 || args.len() > 3 {
            return Err("Wrong number of arguments for LPOP".to_string());
        }
        let count = if args.len() == 3 {
            Some(
                args[2]
                    .parse::<i64>()
                    .map_err(|_| "Invalid count value".to_string())?,
            )
        } else {
            None
        };
        Ok(RedisCommand::LPOP(args[1].clone(), count))
    }

    fn parse_blpop(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 3 {
            return Err("Wrong number of arguments for BLPOP".to_string());
        }
        let timeout = args
            .last()
            .unwrap()
            .parse::<f64>()
            .map_err(|_| "Invalid timeout value".to_string())?;
        // Converting timeout to milliseconds
        let timeout = (timeout * 1000.0) as u64;
        let keys = args[1..args.len() - 1].to_vec();
        Ok(RedisCommand::BLPOP(keys, timeout))
    }

    fn parse_brpop(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 3 {
            return Err("Wrong number of arguments for BRPOP".to_string());
        }
        let timeout = args
            .last()
            .unwrap()
            .parse::<f64>()
            .map_err(|_| "Invalid timeout value".to_string())?;
        // Converting timeout to milliseconds
        let timeout = (timeout * 1000.0) as u64;
        let keys = args[1..args.len() - 1].to_vec();
        Ok(RedisCommand::BRPOP(keys, timeout))
    }

    fn parse_incr(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for INCR".to_string());
        }
        Ok(RedisCommand::INCR(args[1].clone()))
    }

    fn parse_multi(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 1 {
            return Err("Wrong number of arguments for MULTI".to_string());
        }
        Ok(RedisCommand::MULTI)
    }

    fn parse_exec(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 1 {
            return Err("Wrong number of arguments for EXEC".to_string());
        }
        Ok(RedisCommand::EXEC)
    }

    fn parse_discard(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 1 {
            return Err("Wrong number of arguments for DISCARD".to_string());
        }
        Ok(RedisCommand::DISCARD)
    }

    fn parse_zadd(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 4 {
            return Err("Wrong number of arguments for ZADD".to_string());
        }
        let score = args[2]
            .parse::<f64>()
            .map_err(|_| "Invalid score value".to_string())?;
        let member = args[3].clone();
        Ok(RedisCommand::ZADD(args[1].clone(), score, member))
    }

    fn parse_zrank(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 3 {
            return Err("Wrong number of arguments for ZRANK".to_string());
        }
        Ok(RedisCommand::ZRANK(args[1].clone(), args[2].clone()))
    }

    fn parse_zrange(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 4 {
            return Err("Wrong number of arguments for ZRANGE".to_string());
        }
        let start = args[2]
            .parse::<i64>()
            .map_err(|_| "Invalid start index".to_string())?;
        let end = args[3]
            .parse::<i64>()
            .map_err(|_| "Invalid end index".to_string())?;
        Ok(RedisCommand::ZRANGE(args[1].clone(), start, end))
    }

    fn parse_zcard(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for ZCARD".to_string());
        }
        Ok(RedisCommand::ZCARD(args[1].clone()))
    }

    fn parse_zscore(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 3 {
            return Err("Wrong number of arguments for ZSCORE".to_string());
        }
        Ok(RedisCommand::ZSCORE(args[1].clone(), args[2].clone()))
    }

    fn parse_zrem(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 3 {
            return Err("Wrong number of arguments for ZREM".to_string());
        }
        Ok(RedisCommand::ZREM(args[1].clone(), args[2].clone()))
    }

    fn parse_type(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 2 {
            return Err("Wrong number of arguments for TYPE".to_string());
        }
        Ok(RedisCommand::TYPE(args[1].clone()))
    }

    fn parse_xadd(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 4 || args.len() % 2 == 0 {
            return Err("Wrong number of arguments for XADD".to_string());
        }
        let key = args[1].clone();
        let id = args[2].clone();
        let mut fields = Vec::new();
        for i in (3..args.len()).step_by(2) {
            fields.push((args[i].clone(), args[i + 1].clone()));
        }
        Ok(RedisCommand::XADD(key, Some(id), fields))
    }

    fn parse_xrange(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 4 {
            return Err("Wrong number of arguments for XRANGE".to_string());
        }
        let key = args[1].clone();
        let start = args[2].clone();
        let end = args[3].clone();
        Ok(RedisCommand::XRANGE(key, start, end))
    }

    fn parse_xread(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 4 {
            return Err("Wrong number of arguments for XREAD".to_string());
        }

        let mut block_time: Option<u64> = None;
        let mut idx = 1;

        // Check for BLOCK option
        if args[idx].to_uppercase() == "BLOCK" {
            if args.len() < 6 {
                return Err("Wrong number of arguments for XREAD with BLOCK".to_string());
            }
            block_time = Some(
                args[idx + 1]
                    .parse::<u64>()
                    .map_err(|_| "Invalid BLOCK time value".to_string())?,
            );
            idx += 2;
        }

        if args[idx].to_uppercase() != "STREAMS" {
            return Err("Expected 'STREAMS' keyword in XREAD".to_string());
        }

        idx += 1;

        // Remaining arguments should be key-id pairs
        if (args.len() - idx) % 2 != 0 {
            return Err("Wrong number of arguments for XREAD key-id pairs".to_string());
        }

        let mut id_idx = (idx + args.len()) / 2;

        let mut key_id_pairs = Vec::new();
        while idx < id_idx && id_idx < args.len() {
            let key = args[idx].clone();
            let id = args[id_idx].clone();
            key_id_pairs.push((key, id));
            idx += 1;
            id_idx += 1;
        }
        Ok(RedisCommand::XREAD(block_time, key_id_pairs))
    }

    fn parse_geoadd(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() != 5 {
            return Err("Wrong number of arguments for GEOADD".to_string());
        }
        let longitude = args[2]
            .parse::<f64>()
            .map_err(|_| "Invalid longitude value".to_string())?;
        let latitude = args[3]
            .parse::<f64>()
            .map_err(|_| "Invalid latitude value".to_string())?;
        let member = args[4].clone();
        Ok(RedisCommand::GEOADD(
            args[1].clone(),
            longitude,
            latitude,
            member,
        ))
    }

    fn parse_geopos(args: &[String]) -> Result<RedisCommand, String> {
        if args.len() < 3 {
            return Err("Wrong number of arguments for GEOPOS".to_string());
        }
        Ok(RedisCommand::GEOPOS(args[1].clone(), args[2..].to_vec()))
    }
}
