# Bottle 🍼

> **A minimal, modular Kubernetes designed from day one to run on 3–20 machines.**

Bottle is a clean-slate, lightweight distributed orchestration engine built in Go. Rather than stripping down Kubernetes ("Kubernetes Lite"), Bottle answers a fundamental question:

***"What if Kubernetes had been designed to run on 3–20 machines from day one?"***

No heavy external dependencies like etcd or HashiCorp Raft—every layer from custom TCP/Protobuf RPC to cluster membership, Raft consensus, MVCC key-value storage, scheduling, and process execution is built from first principles.

---

## 🏗 System Architecture Roadmap

```
Linux
  ↓
Networking
  ↓
RPC (Phase 0 - Built)
  ↓
Cluster Membership (Phase 1 - Built)
  ↓
Consensus / Raft (Phase 2 - Next)
  ↓
Distributed KV Store (Phase 3)
  ↓
Watch API (Phase 4)
  ↓
API Server (Phase 5)
  ↓
Scheduler (Phase 6)
  ↓
Controller Runtime (Phase 7)
  ↓
Worker Agent (Phase 8)
  ↓
Networking / Service Discovery (Phase 9)
  ↓
pebctl CLI (Phase 10)
  ↓
Bottle Unified Binary (Phase 11)
```

---

## ⚡ Current Progress Summary

### Phase 0 — Foundation & RPC Layer (`rpc/`) ✅ Completed
* **Framed TCP Transport**: Custom binary frame layer over raw TCP sockets (`rpc/frame.go`).
* **Protobuf Envelope**: Universal RPC message envelope carrying `ID`, `Method`, `Headers`, and `Payload []byte` (`rpc/proto/`).
* **Unary & Streaming RPCs**: Full support for single request-response calls and long-lived server streams (`rpc/stream.go`).
* **Connection Pooling**: Global client connection pool (`GlobalPool`) reusing established TCP sockets across nodes (`rpc/connection.go`).
* **Middleware Support**: Interceptor pipeline for authentication, logging, and error handling (`rpc/handler.go`).

### Phase 1 — Cluster Membership (`cluster/`) ✅ Completed
* **Node State Machine**: Manages node lifecycle states (`JOINING`, `ACTIVE`, `SUSPECT`, `DEAD`, `LEFT`).
* **Seed Node Joining**: Seed join handshake where new nodes dynamically discover `ClusterID` and full node topology (`cluster/cluster.go`).
* **Asynchronous Parallel Broadcasting**: Non-blocking `BroadCast` mechanism delivering topological changes to peers concurrently.
* **Bi-directional Heartbeats & Ticker**: 5-second periodic health check loop exchanging state between peers with `context.WithTimeout(2s)`.
* **Failure Detector**: Tracks missed heartbeats per node—automatically transitions un-responsive peers to `DEAD` after 3 consecutive missed pings.
* **Graceful Exit**: `Leave()` protocol notifying the cluster mesh before process termination.

---

## 📁 Repository Structure

```
bottle/
├── rpc/                # Phase 0: Custom TCP transport & Protobuf RPC framework
│   ├── client.go       # RPC client & call dispatcher
│   ├── server.go       # RPC listener & handler router
│   ├── connection.go   # TCP connection management & pooling
│   ├── frame.go        # Binary frame parser
│   ├── handler.go      # Request handlers & middleware chains
│   ├── message.go      # RPC envelope constructor helpers
│   └── proto/          # Protobuf definitions
├── cluster/            # Phase 1: Cluster membership & failure detection
│   ├── node.go         # Node metadata & state machine enums
│   ├── cluster.go      # Thread-safe cluster map, join, broadcast, & heartbeat loop
│   └── handler.go      # RPC endpoints (cluster.join, cluster.update, cluster.heartbeat)
├── docs/               # System architecture & design specifications
│   ├── architecture.md # Overall system architecture blueprint
│   ├── cluster.md      # Detailed cluster membership specification
│   └── rpc.md          # RPC module specification
├── main.go             # Single binary entrypoint
├── go.mod
└── README.md
```

---

## 🚀 Quickstart Example

### Initializing a Cluster Node

```go
package main

import (
	"fmt"
	"github.com/jsndz/bottle/cluster"
	"github.com/jsndz/bottle/rpc"
)

func main() {
	// 1. Create local node identity
	self := cluster.NewNode("192.168.1.10:8000")

	// 2. Initialize RPC Server
	srv := rpc.NewServer("192.168.1.10:8000")

	// 3. Initialize Cluster state
	cls := cluster.NewCluster(self, srv, "bottle-cluster-01")

	// 4. Start background failure detector
	cls.HeartbeatTicker()

	fmt.Println("Bottle cluster node initialized:", cls.ID)
}
```

---

## 📚 Documentation
For deep-dive architectural specifications, inspect the documents in `docs/`:
* 📘 [Overall System Architecture](docs/architecture.md)
* 📗 [Cluster Membership Architecture](docs/cluster.md)
* 📙 [RPC Layer Architecture](docs/rpc.md)
