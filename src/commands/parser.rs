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
        if args.len() != 3 {
            return Err("Wrong number of arguments for SET".to_string());
        }
        Ok(RedisCommand::Set(args[1].clone(), args[2].clone()))
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
}