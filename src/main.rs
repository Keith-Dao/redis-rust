mod commands;
mod handler;
mod resp;
mod store;

use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

async fn handle_stream(
    stream: TcpStream,
    store: store::SharedStore,
    register: commands::SharedRegister,
) {
    let mut handler = handler::RespHandler::new(stream);
    handler.run(store, register).await;
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = store::new();

    let commands: Vec<Box<dyn commands::Command>> = vec![
        Box::new(commands::echo::Echo),
        Box::new(commands::get::Get),
        Box::new(commands::ping::Ping),
        Box::new(commands::rpush::Rpush),
        Box::new(commands::set::Set),
    ];

    let mut register = commands::Register::new();
    register.register_multiple(commands);
    let register = Arc::new(RwLock::new(register));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let store = store.clone();
                let register = register.clone();
                tokio::spawn(async move {
                    handle_stream(stream, store, register).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
