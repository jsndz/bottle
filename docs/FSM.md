# Bottle — Understanding the State Machine Before Raft

## Context

Bottle is a lightweight distributed orchestration system being built from scratch in Go. The architecture will eventually contain cluster membership, Raft consensus, a distributed KV store, scheduling, controllers, workers, and networking.

Before implementing Raft, the goal is to understand the role of a state machine through code rather than trying to understand the entire Raft implementation at once.

## My Question and Confusion

The main question was:

> Before implementing Raft, should I implement a state machine first? How does a state machine actually fit into a system with multiple machines communicating with each other?

The initial confusion was around what exactly the "state machine" represents.

Since Bottle is intended to manage containers and workloads, it was natural to think:

> If Kubernetes manages multiple containers, is each container a finite state machine?

The answer is no.

A container is not the state machine that Raft is replicating.

The state machine represents the **cluster's authoritative state**.

For example, the cluster might have state such as:

```text
deployment/web:
    replicas = 3

pod/a:
    status = running

pod/b:
    status = running

pod/c:
    status = pending
```

Raft does not directly replicate running containers. It replicates the ordered commands that change this state.

---

# 1. What Is the State Machine?

At the simplest level:

```text
Command → State Machine → New State
```

A state machine has:

```text
Current State
     +
  Command
     ↓
New State
```

For Bottle, the first version can be extremely simple:

```text
State = map[string]string
```

Commands could be:

```text
PUT key value
DELETE key
```

For example:

```text
Initial state:

{}

Command:

PUT name bottle

New state:

{
    "name": "bottle"
}
```

Then:

```text
Command:

PUT version 1

New state:

{
    "name": "bottle",
    "version": "1"
}
```

The important property is determinism:

```text
same initial state
+
same commands
=
same final state
```

That property is critical for Raft.

---

# 2. Where Do Commands Come From?

This was one of the main points of confusion.

A command is simply an instruction describing a desired state change.

For example, a client might eventually send:

```text
PUT deployment/web replicas=5
```

or:

```text
DELETE pod/a
```

or later:

```text
CREATE deployment/web replicas=3
```

The command is data.

It does not need to directly execute anything.

The command first becomes part of the replicated log.

The flow is:

```text
Client
  ↓
Leader
  ↓
Raft Log
  ↓
Replicated to Followers
  ↓
Committed
  ↓
State Machine
  ↓
New Cluster State
```

---

# 3. What Does Raft Actually Do?

The state machine itself does not know anything about distributed systems.

It does not know:

* who the leader is
* which machine is alive
* how to communicate with another machine
* how elections work
* what a term is
* how majority voting works

That is Raft's responsibility.

The state machine only knows:

```text
Apply(command)
```

Raft's job is to guarantee that every node eventually receives the same commands in the same order.

For example:

```text
Node A:

PUT x 10
PUT y 20
DELETE x
```

Node B receives:

```text
PUT x 10
PUT y 20
DELETE x
```

Node C receives:

```text
PUT x 10
PUT y 20
DELETE x
```

Each node applies the same sequence to its local state machine.

Therefore, they arrive at the same state.

---

# 4. Concrete Example With Three Machines

Imagine Bottle eventually has:

```text
Node A
Node B
Node C
```

Node A becomes the Raft leader.

A client wants to create a deployment:

```text
CREATE deployment/web replicas=3
```

The client sends this command to Node A.

Node A does not immediately execute the command.

Instead:

```text
Client
   ↓
Node A (Leader)
   ↓
Raft Log
```

Node A replicates the log entry:

```text
Node A → Node B
Node A → Node C
```

Once a majority has accepted the entry:

```text
A + B
```

or:

```text
A + C
```

the entry can become committed.

Then each node applies the command:

```text
Node A → State Machine
Node B → State Machine
Node C → State Machine
```

All three machines now have the same logical cluster state.

---

# 5. The Important Separation

There are three different concepts that should not be mixed together.

## State Machine

Answers:

> "Given this command, how does my state change?"

Example:

```text
PUT replicas 3
```

## Log

Answers:

> "What commands have happened, and in what order?"

Example:

```text
1. PUT replicas 3
2. PUT replicas 5
3. DELETE deployment
```

## Raft

Answers:

> "How do multiple machines agree on that log?"

So:

```text
State Machine
     ↑
     |
   Log
     ↑
     |
   Raft
     ↑
     |
Network
```

Each layer has a different responsibility.

---

# 6. Why Build the State Machine First?

Building the state machine first removes a huge amount of complexity.

start with:

```text
State Machine
```

Then prove:

```text
command 1
command 2
command 3
```

always produces the expected state.

For example:

```text
PUT name bottle
PUT version 1
DELETE name
```

Starting from:

```text
{}
```

should always produce:

```text
{
    "version": "1"
}
```

That gives a concrete foundation before introducing distributed systems.

---

# 7. The First Bottle State Machine

The first implementation should be deliberately boring.


```text
Command → Apply() → State
```

---

# 8. What Comes After the State Machine?

Once the state machine makes sense, add a log.

The next architecture becomes:

```text
Command
   ↓
Log
   ↓
State Machine
```

The log records commands:

```text
Entry 1: PUT name bottle
Entry 2: PUT version 1
Entry 3: DELETE name
```

The state machine can replay the log from the beginning:

```text
Entry 1 → Apply
Entry 2 → Apply
Entry 3 → Apply
```

and reconstruct its state.

This introduces another fundamental distributed-systems concept:

**state can be reconstructed from an ordered sequence of operations.**

---

# 9. Then Introduce Raft

Only after understanding:

```text
Command
   ↓
Log
   ↓
State Machine
```

introduce:

```text
             Raft
              ↓
        Replicated Log
         ↙    ↓    ↘
      Node A Node B Node C
         ↓     ↓     ↓
        SM    SM    SM
```

Raft's job is now much easier to understand.

It is not "the thing that changes the state."

It is the mechanism that makes the replicated log consistent across machines.

---

# 10. The Mental Model to Keep

The simplest mental model for Bottle is:

```text
                    Client
                      |
                      | Command
                      ↓
                 Raft Leader
                      |
                      | replicate
                      ↓
              ┌───────────────┐
              │  Raft Log     │
              └───────────────┘
                 /     |     \
                /      |      \
               ↓       ↓       ↓
             Node A  Node B  Node C
               |       |       |
               ↓       ↓       ↓
           State     State    State
           Machine   Machine  Machine
```

The key rule is:

> Raft decides which commands become committed and in what order. The state machine executes those committed commands.

That distinction is the foundation for understanding the rest of Bottle.

---

# 11. Recommended Implementation Order

For the beginning of Bottle, use this sequence:

```text
1. State Machine
       ↓
2. Command representation
       ↓
3. Append-only Log
       ↓
4. Log replay
       ↓
5. Persistent Log / WAL
       ↓
6. Simple replication simulation
       ↓
7. Raft
       ↓
8. Distributed KV
```

Do not begin with MVCC, transactions, watches, leases, or the full distributed KV layer.

First understand this tiny chain:

```text
Command
   ↓
Log
   ↓
Commit
   ↓
Apply
   ↓
State
```

Once that chain is clear, Raft has somewhere concrete to fit into the architecture.
