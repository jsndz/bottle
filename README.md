# Bottle

> **A minimal, modular Kubernetes designed from day one to run on 3–20 machines.**

Bottle is a clean-slate, lightweight distributed orchestration engine built in Go. Rather than stripping down Kubernetes ("Kubernetes Lite"), Bottle answers a fundamental question:

***"What if Kubernetes had been designed to run on 3–20 machines from day one?"***

No heavy external dependencies like etcd or HashiCorp Raft—every layer from custom TCP/Protobuf RPC to cluster membership, Raft consensus, MVCC key-value storage, scheduling, and process execution is built from first principles.

---

## System Architecture Roadmap

```
Linux
  ↓
Networking
  ↓
RPC (Phase 0 - Built)
  ↓
Cluster Membership (Phase 1 - Built)
  ↓
Consensus / Raft (Phase 2 - Built)
  ↓
Distributed KV Store (Phase 3 - Built)
  ↓
Watch API (Phase 4 - Next)
  ↓
API Server (Phase 5)
  ↓
Scheduler (Phase 6)
  ↓
Worker Agent (Phase 7)
  ↓
Controller Runtime (Phase 8)
  ↓
Networking / Service Discovery (Phase 9)
  ↓
pebctl CLI (Phase 10)
  ↓
Bottle Unified Binary (Phase 11)
```

---

## Current Progress Summary

### Phase 0 — Foundation & RPC Layer (`rpc/`) [Completed]
* **Framed TCP Transport**: Custom binary frame layer over raw TCP sockets (`rpc/frame.go`).
* **Protobuf Envelope**: Universal RPC message envelope carrying `Id`, `Type`, `Method`, `Headers`, and `Payload []byte` (`rpc/proto/`).
* **Unary & Streaming RPCs**: Support for single request-response calls and server streams (`rpc/stream.go`).
* **Connection Pooling**: Thread-safe client connection pool (`GlobalPool` / `ConnPool`) reusing established TCP sockets across nodes (`rpc/connection.go`).
* **Middleware Support**: Interceptor pipeline for authentication, logging, and error handling (`rpc/handler.go`).

### Phase 1 — Cluster Membership (`cluster/`) [Completed]
* **Node State Machine**: Manages node lifecycle states (`JOINING`, `ACTIVE`, `DEAD`).
* **Seed Node Joining**: Seed join handshake where new nodes dynamically discover `ClusterID` and full topology (`cluster/cluster.go`).
* **Asynchronous Parallel Broadcasting**: Non-blocking broadcast mechanism delivering topological changes to peers concurrently.
* **Periodic Heartbeats**: Periodic health check loop exchanging state between peers with context deadlines.
* **Failure Detector**: Tracks missed heartbeats per node—automatically marks unresponsive peers as `DEAD` after consecutive missed pings.
* **Graceful Exit**: `Leave()` protocol notifying the cluster mesh before process termination.

### Phase 2 — Consensus / Raft (`raft/`) [Completed]
* **Leader Election**: Randomized election timers, candidate step-up, and RequestVote RPC handling.
* **Log Replication**: Parallel AppendEntries RPC dispatch with follower log mismatch repair and tracking (`SentLength` and `AckLength`).
* **Commit Quorum**: Majority-based log commit calculation and application to the underlying state machine.
* **State Machine Interface**: Pluggable `FSM` interface for decoupled command execution.
* **Role Transitions**: Dynamic transitions across `Follower`, `Candidate`, and `Leader` with automatic step-down upon discovering higher terms.

### Phase 3 — Distributed KV Store (`kv/`) [Completed]
* **Key-Value State Machine**: Thread-safe in-memory key-value store implementing `raft.FSM`.
* **State Operations**: Mutation support for `PUT` and `DELETE` commands committed via Raft consensus.
* **Read Operations**: Thread-safe concurrent `Get` lookups.

---

## Repository Structure

```
bottle/
├── rpc/                # Phase 0: Custom TCP transport & Protobuf RPC framework
│   ├── client.go       # RPC client & call dispatcher
│   ├── server.go       # RPC listener & handler router
│   ├── connection.go   # TCP connection management & pooling
│   ├── frame.go        # Binary frame parser
│   ├── handler.go      # Request handlers & middleware chains
│   ├── message.go      # RPC envelope constructor helpers
│   ├── rpc_test.go     # RPC unit tests
│   └── proto/          # Protobuf definitions
├── cluster/            # Phase 1: Cluster membership & failure detection
│   ├── node.go         # Node metadata & state machine enums
│   ├── cluster.go      # Thread-safe cluster map, join, broadcast, & heartbeat loop
│   ├── cluster_test.go # Cluster unit tests
│   └── handler.go      # RPC endpoints (cluster.join, cluster.update, cluster.heartbeat)
├── raft/               # Phase 2: Raft consensus engine
│   ├── raft.go         # Raft node state, election loops, and log replication
│   ├── handler.go      # Raft RPC endpoints (raft.election, raft.logs, raft.client.command)
│   ├── types.go        # Raft message types and role constants
│   ├── fsm.go          # State machine interface definition
│   ├── util.go         # Random timeout utilities
│   └── raft_test.go    # Raft unit tests
├── kv/                 # Phase 3: Distributed key-value store
│   ├── kv.go           # Thread-safe in-memory KV store state machine
│   └── kv_test.go      # KV unit tests
├── docs/               # System architecture & design specifications
│   ├── architecture.md # Overall system architecture blueprint
│   ├── cluster.md      # Detailed cluster membership specification
│   ├── FSM.md          # State machine specification
│   └── rpc.md          # RPC module specification
├── main.go             # Single binary entrypoint
├── go.mod
└── README.md
```

---

## Quickstart Example

### Initializing a Cluster Node with Raft

```go
package main

import (
	"fmt"

	"github.com/jsndz/bottle/cluster"
	"github.com/jsndz/bottle/kv"
	"github.com/jsndz/bottle/raft"
	"github.com/jsndz/bottle/rpc"
)

func main() {
	addr := "192.168.1.10:8000"

	// 1. Create local node identity
	self := cluster.NewNode(addr)

	// 2. Initialize RPC Server
	srv := rpc.NewServer(addr)

	// 3. Initialize Cluster state
	cls := cluster.NewCluster(self, srv, "bottle-cluster-01")

	// 4. Initialize KV store and Raft consensus
	kvStore := kv.NewKVStore()
	r := raft.NewRaft(cls)
	r.FSM = kvStore
	r.RegisterHandlers(srv)

	// 5. Start background routines
	cls.HeartbeatTicker()
	r.HeartbeatTicker()

	fmt.Println("Bottle cluster node initialized:", cls.ID)
}
```

---

## Documentation

For deep-dive architectural specifications, inspect the documents in `docs/`:
* [Overall System Architecture](docs/architecture.md)
* [Cluster Membership Architecture](docs/cluster.md)
* [Finite State Machine Architecture](docs/FSM.md)
* [RPC Layer Architecture](docs/rpc.md)
