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
}
