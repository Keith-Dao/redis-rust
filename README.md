# Redis server - Rust

A Rust implementation of a Redis server.

## Features

This Redis server implementation in Rust supports the following commands:

- `PING`: Responds with "PONG".
- `ECHO <message>`: Returns the provided message.
- `SET <key> <value> [PX <milliseconds>]`: Sets the string value of a key.
  - `PX`: Set the specified expire time, in milliseconds.
- `GET <key>`: Get the string value of a key.

## Getting Started

### Prerequisites

Ensure you have Rust and Cargo installed.

### Building the project

Navigate to the root directory of the project and build it using Cargo:

```bash
cargo build
```

### Running the server

You can run the server directly from the target directory or using `cargo run`:

```bash
cargo run
```

The server will start on port `6379` by default.

### Connecting to the server

You can connect to the server using `redis-cli` or any other Redis client:

```bash
redis-cli
```

Once connected, you can try out the supported commands:

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> ECHO "Hello, Rust Redis!"
"Hello, Rust Redis!"
127.0.0.1:6379> SET mykey "myvalue"
OK
127.0.0.1:6379> GET mykey
"myvalue"
127.0.0.1:6379> SET expirekey "temp" EX 10
OK
127.0.0.1:6379> GET expirekey
"temp"
```

## Project Structure

The project is organized as follows:

```
codecrafters-redis-rust/
├── src/
│   ├── commands/             # Individual command implementations (e.g., PING, ECHO, GET, SET)
│   │   ├── echo.rs
│   │   ├── get.rs
│   │   ├── ping.rs
│   │   └── set.rs
│   ├── commands.rs           # Aggregates and dispatches different commands
│   ├── handler.rs            # Handles incoming client connections and command parsing
│   ├── main.rs               # Main entry point of the server
│   ├── resp.rs               # Handles Redis Serialization Protocol (RESP) encoding and decoding
│   └── store.rs              # Manages the key-value store and expiration logic
├── Cargo.toml                # Rust project manifest
├── Cargo.lock                # Dependency lock file
└── README.md                 # This file
```
