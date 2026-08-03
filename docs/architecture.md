# Bottle Overall System Architecture

## 1. Executive Summary

> **Bottle** — *A minimal Kubernetes alternative designed from day one to run on 3–20 machines.*

Rather than relying on heavy third-party dependencies (like etcd or HashiCorp Raft), Bottle is built from the ground up to demonstrate every core architectural concept of container/process orchestration: custom RPC, membership gossip, consensus, MVCC storage, watch events, declarative APIs, scheduling, and reconciliation loops.

---

## 2. Layered Stack Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    CLI Tool (pebctl)                        │
└──────────────────────────────┬──────────────────────────────┘
                               │ Declarative YAML / REST / RPC
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                   Control Plane Engine                      │
│                                                             │
│   ┌───────────────────┐               ┌─────────────────┐   │
│   │    API Server     │◄─────────────►│   Scheduler     │   │
│   └─────────┬─────────┘               └────────┬────────┘   │
│             │                                  │            │
│             ▼                                  ▼            │
│   ┌───────────────────┐               ┌─────────────────┐   │
│   │ Controller Runtime│◄─────────────►│ Event / Watch   │   │
│   └───────────────────┘               └────────┬────────┘   │
│                                                │            │
└──────────────────────────────┬─────────────────┘____________|
                               │ Replicated State             
                               ▼                              
┌─────────────────────────────────────────────────────────────┐
│                  Storage & Consensus Core                   │
│                                                             │
│   ┌───────────────────┐               ┌─────────────────┐   │
│   │ Distributed KV    │◄─────────────►│ Raft Consensus  │   │
│   └───────────────────┘               └─────────────────┘   │
└──────────────────────────────┬──────────────────────────────┘
                               │ Cluster State                │
                               ▼                              │
┌─────────────────────────────────────────────────────────────┐
│                    Infrastructure Base                      │
│                                                             │
│   ┌───────────────────┐               ┌─────────────────┐   │
│   │Cluster Membership │◄─────────────►│ Custom RPC      │   │
│   └───────────────────┘               └─────────────────┘   │
└──────────────────────────────┬──────────────────────────────┘
                               │ Execution                    │
                               ▼                              │
┌─────────────────────────────────────────────────────────────┐
│                  Worker Execution Nodes                     │
│                                                             │
│   ┌───────────────────┐               ┌─────────────────┐   │
│   │ Worker Agent      │               │ Process/OCI Run │   │
│   └───────────────────┘               └─────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Core Architectural Modules

The system is decomposed into 12 modular packages:

### 1. RPC Layer (`rpc/`)
* Custom high-performance TCP transport built with Protobuf serialization.
* Supports both **Unary RPC** (Request/Response) and **Streaming RPC** (long-lived connections).
* Features connection pooling, automatic retries, context cancellation, and keepalive heartbeats.

### 2. Cluster Membership (`cluster/`)
* Manages node discovery, joining, leaving, and failure detection.
* Every process maintains an in-memory map of cluster nodes and its own `self` identity.
* Validates `ClusterID` on joins to isolate clusters.
* Uses background ticker heartbeats (every 2s) to transition nodes: `JOINING` $\rightarrow$ `ACTIVE` $\rightarrow$ `SUSPECT` $\rightarrow$ `DEAD` / `LEFT`.

### 3. Consensus Engine (`raft/`)
* Custom implementation of the **Raft consensus algorithm** (no third-party HashiCorp Raft library).
* Handles leader elections, log replication (`AppendEntries`), vote requests (`RequestVote`), term management, persistence, snapshots, and log compaction.
* Guarantees strong consistency across control plane nodes.

### 4. Distributed KV Store (`kv/`)
* Replicated key-value engine built on top of Raft (similar to etcd).
* Key Features:
  * **MVCC (Multi-Version Concurrency Control)**: Maintains revision history for keys.
  * **Optimistic Concurrency**: Atomic transactions (`TXN`).
  * **Leases**: Automatic key expiration for node heartbeats and lock mechanisms.

### 5. Event System (`watch/`)
* Implements Kubernetes-style resource watch streams.
* Allows subscribers (Scheduler, Controllers, CLI) to watch key prefixes (e.g., `/pods/`) and receive real-time notifications (`PUT`, `DELETE`) with exact revisions.

### 6. API Server (`apiserver/`)
* The central administrative entrypoint for all cluster management.
* Exposes RESTful & RPC APIs for declarative resources:
  * `Node`
  * `Pod`
  * `Deployment`
  * `Service`
* Performs schema validation, authentication, and writes desired state into the Distributed KV store.

### 7. Scheduler (`scheduler/`)
* Decoupled placement engine responsible for binding unassigned Pods to healthy Worker Nodes.
* Execution Pipeline:
  1. **Queue**: Fetch pending pods from API Server.
  2. **Filter**: Remove nodes lacking required CPU/RAM/labels or in `DEAD` state.
  3. **Score**: Rank candidate nodes based on resource availability and load distribution.
  4. **Bind**: Update Pod spec in KV store with target `NodeID`.

### 8. Controller Runtime (`controller-runtime/`)
* Framework for running background **Reconciliation Loops** (`Reconcile(request)`).
* Built-in Controllers:
  * **DeploymentController**: Ensures actual pod replicas match `spec.replicas`.
  * **NodeController**: Evicts pods from `DEAD` nodes and triggers re-scheduling.
  * **ReplicaSetController**: Manages pod lifecycle.

### 9. Worker Agent (`worker/`)
* Daemon process running on worker nodes.
* Watches the API Server for Pods assigned to its own `NodeID`.
* Executes workloads:
  * *Phase 1*: Native OS processes using Go `exec.Command`.
  * *Phase 2*: OCI container isolation or WASM runtimes.
* Reports process status and container health back to API Server.

### 10. Service Discovery & Networking (`network/`)
* Minimal networking mesh for service communication.
* Implements basic internal DNS resolution, Virtual IP (VIP) routing, and simple load balancing across Pod endpoints.

### 11. CLI (`pebctl`)
* Command-line tool for cluster operators.
* Commands:
  * `pebctl apply -f deployment.yaml`
  * `pebctl get pods / nodes`
  * `pebctl delete pod <name>`
  * `pebctl logs <pod-name>`

### 12. Single Binary Orchestrator (`main.go`)
* Consolidates all components into a unified executable (`bottle`).
* Can run as a Control-Plane node, Worker node, or combined single-node dev cluster depending on CLI flags.

---

## 4. End-to-End Control Flow Example

Here is what happens when a user runs `pebctl apply -f nginx.yaml`:

```
User -> pebctl apply -f nginx.yaml
   │
   ▼
[API Server] ──► Validates spec & writes Deployment to [Distributed KV]
   │
   ▼
[Deployment Controller] (via Watch API)
   │ Detects missing replicas
   └─► Creates 2 Pod objects in [Distributed KV] (Node = unassigned)
   │
   ▼
[Scheduler] (via Watch API)
   │ Detects unassigned Pods
   │ Filters & Scores active Nodes
   └─► Binds Pod 1 -> Node A, Pod 2 -> Node B in [Distributed KV]
   │
   ▼
[Worker Agent on Node A & B] (via Watch API)
   │ Sees Pod assigned to self
   └─► Executes `exec.Command("nginx")` & streams status to API Server
```

---

## 5. Directory Mapping

```
Bottle/
├── main.go               # Single binary entrypoint
├── rpc/                  # Custom TCP/Protobuf RPC transport
├── cluster/              # Membership, discovery & heartbeats
├── raft/                 # Custom Raft consensus implementation
├── kv/                   # Replicated MVCC KV store
├── watch/                # Prefix event watch system
├── apiserver/            # REST/RPC API endpoints & validation
├── scheduler/            # Pod placement engine (Queue/Filter/Score/Bind)
├── controller-runtime/   # Reconciler loops (Deployment, Node controllers)
├── worker/               # Execution daemon (exec.Command / process runner)
├── network/              # Service discovery & load balancing
├── cli/                  # pebctl tool codebase
└── docs/                 # System architecture & module specifications
```
