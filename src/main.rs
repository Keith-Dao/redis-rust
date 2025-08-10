#![allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn handle_command(buffer: &[u8; 512], count: usize) -> Result<Vec<u8>, std::str::Utf8Error> {
    let packet = std::str::from_utf8(&buffer[..count])?;
    let tokens = packet.split("\r\n").collect::<Vec<_>>();

    let command = tokens[2];
    if command.eq_ignore_ascii_case("PING") {
        Ok(b"+PONG\r\n".to_vec())
    } else if command.eq_ignore_ascii_case("ECHO") {
        Ok(tokens[3..].join("\r\n").into_bytes())
    } else {
        panic!("Invalid command")
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");

                tokio::spawn(async move {
                    let mut buffer = [0; 512];
                    while let Ok(bytes) = stream.read(&mut buffer).await {
                        if bytes == 0 {
                            break;
                        }

                        match handle_command(&buffer, bytes) {
                            Ok(result) => stream.write_all(&result).await.unwrap(),
                            Err(err) => {
                                println!("Errored: {:?}", err);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
