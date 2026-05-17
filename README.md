# In-Memory Key-Value Store

A Redis-compatible in-memory key-value store built from scratch in C++23. Implements the RESP (Redis Serialization Protocol) and supports a wide range of Redis commands including replication, pub/sub, and transactions.

## Features

- **Core commands** — `PING`, `ECHO`, `SET`, `GET`, `INCR`, `KEYS`, `CONFIG GET`
- **Expiry** — Key expiration via `SET ... PX <milliseconds>`
- **Transactions** — `MULTI` / `EXEC` / `DISCARD` with optimistic locking via `WATCH` / `UNWATCH`
- **Pub/Sub** — `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBSUB NUMSUB`
- **Replication** — Master/replica setup using `REPLCONF` / `PSYNC` / `WAIT`
- **RDB persistence** — Load snapshot files on startup via `--dbfilename`
- **RESP protocol** — Full parser and serializer for the Redis wire protocol
- **Event loop** — Non-blocking I/O using `select()` for concurrent client handling

## Build

Requires CMake and a C++23-compatible compiler.

```sh
cmake -B build
cmake --build build
```

Or use the helper script:

```sh
./your_program.sh
```

## Run

```sh
# Start as master on default port 6379
./server

# Custom port
./server --port 6380

# Load an RDB snapshot
./server --dir /path/to/dir --dbfilename dump.rdb

# Start as a replica
./server --port 6380 --replicaof "127.0.0.1 6379"
```

## Project Structure

```
src/
├── Server.cpp               # Entry point, event loop, socket management
├── CommandHandler.cpp/h     # Command dispatch and handlers
├── RedisStore.cpp/h         # In-memory store with expiry and watchers
├── RespProtocol.cpp/h       # RESP serialization / parsing
├── ReplicationManager.cpp/h # Master-replica replication logic
├── PubSubManager.cpp/h      # Pub/Sub channel management
├── RDBParser.cpp/h          # RDB file parser
└── ClientState.h            # Per-client state (MULTI queue, pub/sub mode, etc.)
```

## Tech Stack

- **Language:** C++23
- **Build:** CMake + vcpkg
- **Networking:** POSIX sockets, `select()`-based event loop
- **Protocol:** RESP (Redis Serialization Protocol)
