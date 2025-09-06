mod handler;
mod resp;
mod store;

use tokio::net::{TcpListener, TcpStream};

async fn handle_stream(stream: TcpStream, store: store::Store) {
    let mut handler = handler::RespHandler::new(stream);
    handler.run(store).await;
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = store::new();

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
