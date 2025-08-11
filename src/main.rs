#![allow(unused_imports)]
use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

async fn handle_stream(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);

    while let Ok(Some(message)) = handler.read_stream().await {
        let (command, args) = extract_command(message).unwrap();
        let response = match command.to_lowercase().as_str() {
            "ping" => resp::RespType::BulkString("PONG".to_string()),
            "echo" => args[0].clone(),
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

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    handle_stream(stream).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
