#![allow(unused_imports)]
use anyhow::Result;
use std::boxed::Box;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

mod resp;

fn unpack_string(message: &resp::RespType) -> Result<String> {
    match message {
        resp::RespType::BulkString(s) | resp::RespType::SimpleString(s) => Ok(s.clone()),
        _ => Err(anyhow::anyhow!("Cannot unpack: {:?}", message)),
    }
}

fn extract_command(message: resp::RespType) -> Result<(String, Vec<resp::RespType>)> {
    match message {
        resp::RespType::Array(vec) => Ok((
            unpack_string(&vec[0]).unwrap(),
            vec.into_iter().skip(1).collect(),
        )),
        _ => Err(anyhow::anyhow!("Invalid command: {:?}", message)),
    }
}

type Store = Arc<Mutex<Box<HashMap<String, (String, Option<Instant>)>>>>;

async fn handle_set(args: Vec<resp::RespType>, store: &Store) -> resp::RespType {
    match args.as_slice() {
        [key, value] | [key, value, ..] => {
            let (key, value) = match (unpack_string(key), unpack_string(value)) {
                (Ok(key), Ok(value)) => (key, value),
                _ => panic!("Invalid SET arguments: {:?}", args),
            };

            let deletion_instant = match args.get(2..4) {
                Some([param, duration]) => match (unpack_string(param), unpack_string(duration)) {
                    (Ok(param), Ok(duration)) if param.to_lowercase() == "px" => {
                        match duration.parse::<u64>() {
                            Ok(milliseconds) => {
                                Some(Instant::now() + Duration::from_millis(milliseconds))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                },
                _ => None,
            };

            let mut store = store.lock().await;
            store.insert(key, (value, deletion_instant));

            resp::RespType::SimpleString("OK".to_string())
        }

        _ => panic!("Invalid"),
    }
}

async fn handle_get(args: Vec<resp::RespType>, store: &Store) -> resp::RespType {
    match args.as_slice() {
        [key] | [key, ..] => {
            let key = match unpack_string(key) {
                Ok(key) => key,
                _ => panic!("Invalid GET arguments: {:?}", args),
            };

            let mut store = store.lock().await;
            match store.get(&key) {
                Some((value, deletion_time)) => match deletion_time {
                    Some(deletion_time) if deletion_time <= &Instant::now() => {
                        store.remove(&key);
                        resp::RespType::Null()
                    }
                    _ => resp::RespType::BulkString(value.clone()),
                },
                None => resp::RespType::Null(),
            }
        }

        _ => panic!("Invalid"),
    }
}

async fn handle_stream(stream: TcpStream, store: Store) {
    let mut handler = resp::RespHandler::new(stream);

    while let Ok(Some(message)) = handler.read_stream().await {
        let (command, args) = extract_command(message).unwrap();
        let response = match command.to_lowercase().as_str() {
            "ping" => resp::RespType::SimpleString("PONG".to_string()),
            "echo" => args[0].clone(),
            "set" => handle_set(args, &store).await,
            "get" => handle_get(args, &store).await,
            _ => panic!("Invalid redis command: {:?}", command),
        };

        handler.write_stream(response).await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(Mutex::new(std::boxed::Box::new(
        std::collections::HashMap::new(),
    )));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let store = store.clone();
                tokio::spawn(async move {
                    handle_stream(stream, store).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
