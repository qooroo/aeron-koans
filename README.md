# aeron-koans

A series of self-contained, runnable mini-applications that teach core
[Aeron](https://github.com/real-logic/aeron) concepts using
[SBE](https://github.com/real-logic/simple-binary-encoding) for message encoding.

---

## Requirements

| Tool | Version |
|------|---------|
| JDK  | 17+     |
| Gradle | bundled via `./gradlew` |

---

## Build

```bash
./gradlew build
```

SBE codecs are generated automatically from
`src/main/resources/sbe/messages.xml` during the build.

---

## Koan 1 – Aeron Messaging (Point-to-Point with SBE)

Two separate Aeron instances communicate via UDP.

* **MessagingVenue** – subscribes for `NewOrderSingle` orders, decodes the SBE
  payload, and sends back an `ExecutionReport`.
* **MessagingClient** – encodes a `NewOrderSingle` using SBE, publishes it to
  the Venue, and decodes the incoming `ExecutionReport`.

Messages use minimal FIX models (`NewOrderSingle` and `ExecutionReport`) encoded
with SBE.

### Run

Open two terminals:

```bash
# Terminal 1 – start Venue first
./gradlew runMessagingVenue

# Terminal 2 – send an order
./gradlew runMessagingClient
```

---

## Koan 2 – Aeron Archive (Record, Store & Replay)

Three applications demonstrate recording messages to Aeron Archive and later
replaying them.

* **ArchiveServer** – receives `Ping` messages from the Client over UDP,
  **writes each incoming Ping to its local Aeron Archive**, and sends `Pong`
  replies.
* **ArchiveClient** – sends five `Ping` messages, **records the incoming Pong
  replies to its own local Aeron Archive**, then waits until interrupted so its
  archive remains accessible.
* **ArchiveReader** – connects to either the Server's or the Client's running
  archive, lists all recordings, replays each one, and decodes/logs every SBE
  message to the console.

### Run

Open three terminals in order:

```bash
# Terminal 1 – start Server (keeps running)
./gradlew runArchiveServer

# Terminal 2 – start Client (sends 5 pings, then waits)
./gradlew runArchiveClient

# Terminal 3 – read recordings from the Server archive
./gradlew runArchiveReader

# … or read from the Client archive:
./gradlew runArchiveReader --args="client"
```

Press `Ctrl-C` in terminals 1 and 2 when you are done.

---

## Koan 3 – Aeron Cluster (Replicated Named-Counter Map)

Three cluster nodes form a Raft consensus group hosting a replicated `HashMap<String, Long>`.
A client sends named-counter commands and receives SBE-encoded responses. Demonstrates
ingress/egress, leader election, unhappy-path error handling, snapshots, and failover.

* **ClusterNode** – one class, three processes (args `0`, `1`, `2`). Each runs an embedded
  `ClusteredMediaDriver` (MediaDriver + Archive + ConsensusModule) and a
  `ClusteredServiceContainer` hosting `CounterMapService`.
* **CounterMapService** – implements `ClusteredService`. All state-machine logic, snapshot
  serialisation, and snapshot restore live here.
* **ClusterClient** – interactive REPL. Reads commands from stdin, sends SBE-encoded
  `ClusterCommand` messages, prints SBE-encoded `ClusterResponse` replies.
* **ClusterSnapshot** – requests a snapshot from the running leader via `ClusterTool`.

### Run

Open **four** terminals in order:

```bash
# Terminal 1
./gradlew runClusterNode0

# Terminal 2
./gradlew runClusterNode1

# Terminal 3
./gradlew runClusterNode2

# Terminal 4 – once all three nodes print "Running"
./gradlew runClusterClient
```

**Available commands:**

| Command | Effect |
|---------|--------|
| `inc <key> [delta]` | Increment counter (creates at 0 if new; default delta=1) |
| `dec <key> [delta]` | Decrement counter (`KEY_NOT_FOUND` if absent, `UNDERFLOW` if out of range) |
| `reset <key>` | Set counter to 0 (`KEY_NOT_FOUND` if absent) |
| `get <key>` | Read current value |
| `del <key>` | Remove counter, returns last value |
| `quit` | Exit the client |

### Failover walkthrough

1. Run some commands and note the values.
2. The client prints `[Leader] node=N` — press `Ctrl-C` on that node's terminal.
3. Watch the remaining two nodes elect a new leader.
4. Continue sending commands — cluster operates normally with 2/3 nodes up.
5. Restart the killed node — it catches up from the replicated log.

### Snapshot walkthrough

1. Run several commands to populate counters.
2. In a fifth terminal, request a snapshot: `./gradlew runClusterSnapshot`
3. Stop all three nodes (`Ctrl-C` in each terminal).
4. Restart all three nodes, then the client.
5. Run `get <key>` — your counters are restored from the snapshot.

---

## SBE Schema

All messages are defined in `src/main/resources/sbe/messages.xml`.

| Template ID | Message            | Used in         |
|-------------|--------------------|-----------------|
| 1           | `NewOrderSingle`   | Koan 1          |
| 2           | `ExecutionReport`  | Koan 1          |
| 3           | `Ping`             | Koan 2          |
| 4           | `Pong`             | Koan 2          |
| 5           | `ClusterCommand`   | Koan 3          |
| 6           | `ClusterResponse`  | Koan 3          |
| 7           | `SnapshotHeader`   | Koan 3          |
| 8           | `SnapshotEntry`    | Koan 3          |

---

## Project structure

```
src/
  main/
    resources/sbe/messages.xml          ← SBE schema
    java/io/aeron/koans/
      messaging/
        MessagingClient.java            ← Koan 1: client (sends orders)
        MessagingVenue.java             ← Koan 1: venue  (fills orders)
      archive/
        ArchiveServer.java              ← Koan 2: server (records Pings)
        ArchiveClient.java              ← Koan 2: client (records Pongs)
        ArchiveReader.java              ← Koan 2: replays archive recordings
      cluster/
        ClusterNode.java                ← Koan 3: cluster node (run 3×)
        CounterMapService.java          ← Koan 3: replicated state machine
        ClusterClient.java              ← Koan 3: interactive client
        ClusterSnapshot.java            ← Koan 3: request snapshot from leader
        Result.java                     ← Koan 3: state machine result record
build/generated/sbe/main/java/          ← SBE-generated codecs (auto-generated)
```
