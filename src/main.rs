#![allow(unused_imports)]
use anyhow::Result;
use std::boxed::Box;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

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

async fn handle_set(
    args: Vec<resp::RespType>,
    store: &Arc<RwLock<Box<HashMap<String, String>>>>,
) -> resp::RespType {
    let num_required_args = 2;
    if args.len() < num_required_args {
        panic!(
            "Invalid number of SET arguments. Expected {}, got: {}",
            num_required_args,
            args.len()
        );
    }

    let mut store = store.write().await;
    let (key, value) =
        if let (Ok(key), Ok(value)) = (unpack_string(&args[0]), unpack_string(&args[1])) {
            (key, value)
        } else {
            panic!("Invalid SET arguments: {:?}", args);
        };
    store.insert(key, value);

    resp::RespType::SimpleString("OK".to_string())
}

async fn handle_get(
    args: Vec<resp::RespType>,
    store: &Arc<RwLock<Box<HashMap<String, String>>>>,
) -> resp::RespType {
    let num_required_args = 1;
    if args.len() < num_required_args {
        panic!(
            "Invalid number of GET arguments. Expected {}, got: {}",
            num_required_args,
            args.len()
        );
    }

    let store = store.read().await;
    let key = if let Ok(key) = unpack_string(&args[0]) {
        key
    } else {
        panic!("Invalid GET arguments: {:?}", args);
    };

    match store.get(&key) {
        Some(value) => resp::RespType::BulkString(value.clone()),
        None => resp::RespType::Null(),
    }
}

async fn handle_stream(stream: TcpStream, store: Arc<RwLock<Box<HashMap<String, String>>>>) {
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
    let store = Arc::new(RwLock::new(std::boxed::Box::new(
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
