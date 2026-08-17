## 1. Data Model & Node States (node.go)

  [ ] Add SUSPECT State Enum:
  Missing SUSPECT in NodeState (currently only has DEAD, JOINING, LEFT, ACTIVE).
  [ ] Track Missed Heartbeats:
  Add a MissedHeartbeats int or Failures int field on Node to support the multi-stage failure detector (ACTIVE →
  SUSPECT → DEAD).
  ──────
  ### 2. Payload DTOs for Clean JSON Serialization
  Currently, cluster.go and handler.go try to serialize the Cluster struct directly (json.Marshal(c)), which fails
  because id and nodes are unexported private fields.
  [ ] Create dedicated request/response DTOs (can be in cluster/dto.go or cluster/cluster.go):
    type JoinRequest struct {
        ClusterID string `json:"cluster_id"`
        Node      *Node  `json:"node"`
    }

    type JoinResponse struct {
        ClusterID string           `json:"cluster_id"`
        Nodes     map[string]*Node `json:"nodes"`
    }

    type HeartbeatPayload struct {
        ClusterID string    `json:"cluster_id"`
        SenderID  string    `json:"sender_id"`
        SentAt    time.Time `json:"sent_at"`
    }

  ──────
  ### 3. Cluster Lifecycle & Logic (cluster.go)

  [ ] Update NewCluster Constructor:
  Accept clusterID string (e.g. NewCluster(clusterID string, self *Node, srv *rpc.Server)) instead of always
  generating a random UUID.
  [ ] Fix Join(seedAddr string):
      1. Pre-flight ping: Check if seedAddr is alive before attempting join.
      2. Context Timeout: Replace context.Background() with a timeout (e.g. 5s).
      3. Fix method casing: Fix "cluster.Join" → "cluster.join".
      4. Deserialize JoinResponse: Properly populate c.id, c.nodes, and mark c.self.State = ACTIVE.
  [ ] Fix Heartbeat() Loop & Race Conditions:
      1. Remove early return: Don't break the loop when one peer is unreachable; continue processing all peers.
      2. Use sync.WaitGroup: Concurrently ping all peers in parallel.
      3. Thread Safety: Lock c.mu.Lock() when updating c.nodes[node.ID].State and LastPing.
      4. Context Deadline: Add a strict 1–2s timeout on peer pings.
      5. State Progression: Transition active → suspect → dead based on missed counts.
  [ ] Fix Leave():
      1. Lock c.mu.Lock() before updating state.
      2. Mark c.self.State = LEFT.
      3. Cleanly broadcast "cluster.left" to peers.
  [ ] Add Thread-safe Helper Methods:
      • GetNodes() []*Node
      • GetNode(id string) (*Node, bool)
      • GetID() string
      • NodeExists(id string) bool

  ──────
  ### 4. RPC Handlers (handler.go)

  [ ] Enhance HandleJoin:
      1. Cluster ID Verification: Verify req.ClusterID == c.id.
      2. Duplicate Check: Ensure node ID isn't already active on a conflicting address.
      3. Reverse Ping Verification: Ping the joining node's advertised address before accepting it.
      4. Return JoinResponse: Send the full node map back in JoinResponse.
  [ ] Fix HandleUpdate:
  Unmarshal JoinResponse instead of unexported Cluster struct.
  [ ] Fix HandleHeartBeat:
      1. Validate ClusterID.
      2. Update the sender node's LastPing and State = ACTIVE in c.nodes (currently it updates c.self.LastPing by
      mistake).

  ──────
  ### 5. Prerequisites in rpc (Affects Cluster)

  [ ] Rename Port → Addr in rpc.Client: Ensure net.Dial("tcp", c.Addr) works with host:port.
  [ ] Add client.Ping(ctx): Low-level heartbeat frame exchange over the connection pool.
  [ ] Fix Nil Map Panic in client.Call(): Ensure headers isn't nil before assigning headers["X-Timeout"].
  ──────