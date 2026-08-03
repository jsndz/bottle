Cluster is group of nodes(here)
So how do you create a cluster? how to join one?
When a node is created it will have its own cluster.
It is general rule like it may look like not much of cluster since there is only one member
but still a cluster and other things will join the cluster later

Id will be considered for cluster if there can multiple group/cluster

So each node will be a bottle binary running and to join any cluster pass the seed which will be responsible for accepting or rejecting the node
Join is initiated by the node, so it will call the cluster.Join method as a client Server should respond with either ok or no

# Cluster Membership Architecture (`cluster/`)

## 1. Overview & Philosophy

The `cluster` module in Bottle is responsible for **membership management, discovery, and failure detection** across a set of 3–20 nodes forming a single distributed system.

Before any higher-level abstractions like consensus (Raft), distributed KV storage, or workload scheduling can function, every machine must maintain a coherent, dynamically updated view of **who is in the cluster** and **who is alive**.

---

## 2. Process & Deployment Model

### 2.1 The "One Binary per Machine" Model
* Each physical host, Virtual Machine, or container runs a single instance of the `bottle` binary.
* A cluster of $N$ machines means $N$ independent OS processes running concurrently, each listening on a designated TCP address (`host:port`).
* Each process manages its own in-memory `Cluster` state.

```
┌─────────────────────────────────────────────────────────────┐
│                      Bottle Cluster                         │
│                                                             │
│   ┌─────────────────┐             ┌─────────────────┐       │
│   │ Node A (Seed)   │ ◄───RPC───► │ Node B          │       │
│   │ 192.168.1.1:8000│             │ 192.168.1.2:8000│       │
│   └────────┬────────┘             └────────┬────────┘       │
│            │                               │                │
│            └──────────────┬────────────────┘                │
│                           │ RPC                             │
│                           ▼                                 │
│                   ┌─────────────────┐                       │
│                   │ Node C          │                       │
│                   │ 192.168.1.3:8000│                       │
│                   └─────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 The "Self Node" Concept
Within any running process, the node must distinguish between itself and its peers:
* **`self`**: The `Node` struct representing the current local machine (e.g., Node A on host `192.168.1.1`).
* **`peers`**: The map of all other remote nodes in the cluster (`nodes[node_id]`).

By knowing `self`, a process avoids sending unnecessary TCP RPC messages to itself and correctly identifies itself during network handshakes.

---

## 3. Cluster Identity & Security (`ClusterID`)

To prevent accidental cluster merging (e.g., a development node joining a production cluster by mistake), every cluster possesses a unique **`ClusterID`** (e.g., `bottle-prod-east`).

### Join Validation
During the initial handshake RPC between a joining node and a seed node:
1. The seed node verifies that the joining request matches the target `ClusterID`.
2. If `ClusterID` matches, the seed accepts the node and returns the current cluster membership topology.
3. If `ClusterID` mismatches, the join operation is rejected with an explicit error.

---

## 4. Node Lifecycle & States

Every node in the cluster transitions through a state machine:

```
  [ New / Bootstrapping ]
             │
             ▼
        (StateJoining) ──[ Successful Handshake ]──► (StateActive)
             │                                            │
             │ [Ping Timeout]                             │ [Missed 3 Heartbeats]
             ▼                                            ▼
        (StateDead) ◄────────────────────────────── (StateSuspect)
                                                          │
                                                          │ [Graceful Shutdown]
                                                          ▼
                                                     (StateLeft)
```

| State | Description |
| :--- | :--- |
| `JOINING` | Node is attempting to contact a seed node and perform initial handshake. |
| `ACTIVE` | Node is fully joined, healthy, and exchanging heartbeats. |
| `SUSPECT` | Missed heartbeats; failure detector suspecting the node may be offline. |
| `DEAD` | Failure detector confirmed node unresponsive; node evicted from active scheduling. |
| `LEFT` | Node initiated a graceful shutdown (`Leave()`) and exited clean. |

---

## 5. Core Data Structures & Interfaces

### 5.1 `Node` Struct (`cluster/node.go`)
Represents the metadata of a single node in the cluster.

```go
package cluster

import "time"

type NodeState string

const (
	StateJoining NodeState = "JOINING"
	StateActive  NodeState = "ACTIVE"
	StateSuspect NodeState = "SUSPECT"
	StateDead    NodeState = "DEAD"
	StateLeft    NodeState = "LEFT"
)

type Node struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"` // e.g. "192.168.1.2:8000"
	State         NodeState `json:"state"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}
```

### 5.2 `Cluster` Struct (`cluster/cluster.go`)
Maintains the thread-safe state map of all nodes in the cluster.

```go
package cluster

import (
	"sync"
	"time"
)

type Cluster struct {
	mu        sync.RWMutex
	clusterID string
	self      *Node
	nodes     map[string]*Node
}

func NewCluster(clusterID string, self *Node) *Cluster {
	return &Cluster{
		clusterID: clusterID,
		self:      self,
		nodes:     map[string]*Node{self.ID: self},
	}
}
```

---

## 6. Protocols & Workflows

### 6.1 Cluster Join Sequence

```
Joining Node (Node B)                     Seed Node (Node A)
        │                                         │
        │─── 1. JoinRPC(ClusterID, NodeB_Info) ──►│
        │                                         │ Check ClusterID matches
        │                                         │ Add Node B to nodes map
        │◄── 2. JoinResponse(NodesList, Status) ──│
        │                                         │
Adopts NodesList & ClusterID                       │
Sets State = ACTIVE                               │
```

### 6.2 Heartbeat & Failure Detection Loop

* **Heartbeat Interval**: Every `2` seconds, each node sends a lightweight `HeartbeatRPC` to all active peers (or a gossip subset).
* **Timeout Threshold**: If a node fails to respond for `10` seconds (5 missed heartbeats):
  1. Transition state from `ACTIVE` to `SUSPECT`.
  2. If unrecovered after another `10` seconds, transition to `DEAD`.
  3. Evict/notify upstream controllers (Raft/Scheduler) to re-balance workloads.

### 6.3 Graceful Leave Sequence

When a node shuts down cleanly (`pebctl node leave` or SIGTERM):
1. Node sends a `LeaveRPC` to all peers.
2. Peers update Node state to `LEFT` in their maps.
3. Node closes network connections and exits process safely without triggering false failure alarms.

---

## 7. Next Implementation Steps

1. Implement `cluster/node.go` (Struct & state enums).
2. Implement `cluster/cluster.go` (`NewCluster`, `AddNode`, `RemoveNode`, `GetNodes`).
3. Define Protobuf messages in `rpc/proto/cluster.proto` for `JoinRPC`, `HeartbeatRPC`, `LeaveRPC`.
4. Build `cluster/heartbeat.go` for background ping tickers and failure detection timers.


