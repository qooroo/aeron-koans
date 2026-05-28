# Koan 3 – Aeron Cluster Design

**Date:** 2026-05-28  
**Status:** Approved

---

## Overview

A new koan demonstrating Aeron Cluster: a 3-node Raft cluster hosting a replicated named-counter map, with a single interactive client. Covers ingress/egress, consensus, unhappy paths, snapshots, and failover.

---

## Architecture

Four programs, four terminals:

```
ClusterClient
    │
    │  ingress (UDP)
    ▼
ClusterNode 0  ◄──── Raft log replication ────► ClusterNode 1
                                                 ClusterNode 2
```

- **`ClusterNode`** — single parameterized `main()`, launched three times with arg `0`, `1`, or `2`. Each node runs a `ClusteredMediaDriver` (embedded MediaDriver + Archive + ConsensusModule) and a `ClusteredServiceContainer` hosting `CounterMapService`. Ports are derived from `nodeId` (base 20000, stride 100).
- **`CounterMapService`** — implements `ClusteredService`. Maintains a `HashMap<String, Long>`. Handles `onSessionMessage`, `onTakeSnapshot`, `onLoadSnapshot`.
- **`ClusterClient`** — connects to all three ingress endpoints. Reads commands from stdin, sends SBE-encoded `ClusterCommand`, receives SBE-encoded `ClusterResponse`.

### Port Layout

| Node | Archive control | Raft log (internal) | Member endpoint | Ingress endpoint |
|------|----------------|---------------------|-----------------|-----------------|
| 0    | 20000          | 20001               | 20002           | 20003           |
| 1    | 20100          | 20101               | 20102           | 20103           |
| 2    | 20200          | 20201               | 20202           | 20203           |

Client ingress URI: `aeron:udp?endpoint=localhost:20003,localhost:20103,localhost:20203`

---

## State Machine

`CounterMapService` holds `HashMap<String, Long> counters` as its full replicated state.

| Command | Key exists? | Result |
|---------|-------------|--------|
| `INCREMENT key [delta]` | No | Auto-creates at 0, then adds delta |
| `INCREMENT key [delta]` | Yes | Adds delta; `OVERFLOW` if result exceeds `Long.MAX_VALUE` |
| `DECREMENT key [delta]` | No | `KEY_NOT_FOUND` |
| `DECREMENT key [delta]` | Yes | Subtracts delta; `UNDERFLOW` if result below `Long.MIN_VALUE` |
| `RESET key` | No | `KEY_NOT_FOUND` |
| `RESET key` | Yes | Sets value to 0, returns `OK` |
| `GET key` | No | `KEY_NOT_FOUND` |
| `GET key` | Yes | Returns current value |
| `DELETE key` | No | `KEY_NOT_FOUND` |
| `DELETE key` | Yes | Removes entry, returns last value |
| Any | — | `INVALID_DELTA` if delta ≤ 0 |

Delta defaults to 1 if omitted.

---

## SBE Messages

Six new definitions added to `src/main/resources/sbe/messages.xml`:

### New enums

```xml
<enum name="CommandType" encodingType="uint8">
    INCREMENT=0, DECREMENT=1, RESET=2, GET=3, DELETE=4
</enum>

<enum name="ResponseStatus" encodingType="uint8">
    OK=0, KEY_NOT_FOUND=1, UNDERFLOW=2, OVERFLOW=3, INVALID_DELTA=4
</enum>
```

### New messages

| Template ID | Name | Fields |
|-------------|------|--------|
| 5 | `ClusterCommand` | `CommandType type`, `int64 delta`, `varAscii key` |
| 6 | `ClusterResponse` | `ResponseStatus status`, `int64 value`, `varAscii key`, `varAscii error` |
| 7 | `SnapshotHeader` | `int32 count` |
| 8 | `SnapshotEntry` | `int64 value`, `varAscii key` |

`SnapshotHeader` and `SnapshotEntry` are only written to/read from the snapshot publication — never sent to clients.

---

## Snapshots

`onTakeSnapshot(Publication pub)`:
1. Offer one `SnapshotHeader` with `count = counters.size()`
2. Offer one `SnapshotEntry` per map entry

`onLoadSnapshot(Image image)`:
1. Poll one `SnapshotHeader`, read `count`
2. Poll `count` `SnapshotEntry` messages, rebuild the map

Snapshots are triggered automatically via `snapshotIntervalNs = 30_000_000_000L` (30 s) on `ConsensusModule.Context`.

**Observable demo:** send several commands, wait ~30 s for a snapshot, kill all three nodes, restart them, verify counters are restored.

---

## Failover

No extra code needed beyond what `ClusteredService` provides. The `EgressListener` prints `[Leader] node=N` whenever leadership changes.

**Observable demo:**
1. Send commands, note values
2. Kill the leader terminal (`Ctrl-C`)
3. Watch the two remaining nodes elect a new leader
4. Send more commands — cluster keeps working with 2/3 nodes
5. Restart the killed node — it catches up from the log

---

## Client Interface

Stdin commands accepted by `ClusterClient`:

```
inc <key> [delta]    → INCREMENT
dec <key> [delta]    → DECREMENT
reset <key>          → RESET
get <key>            → GET
del <key>            → DELETE
quit                 → exit
```

Response format: `[OK] visits = 42` or `[ERROR] KEY_NOT_FOUND: visits`  
Leadership change: `[Leader] node=1`

---

## File Structure

```
src/main/java/io/aeron/koans/cluster/
  ClusterNode.java           — main(args), arg 0/1/2 selects node id
  CounterMapService.java     — ClusteredService implementation + snapshot logic
  ClusterClient.java         — interactive stdin client
```

---

## Gradle Tasks

Four new run tasks added to `build.gradle.kts`:

```
runClusterNode0   → ClusterNode  (args: ["0"])
runClusterNode1   → ClusterNode  (args: ["1"])
runClusterNode2   → ClusterNode  (args: ["2"])
runClusterClient  → ClusterClient
```

---

## Dependencies

No new dependencies. `aeron-all:1.44.0` already bundles `aeron-cluster`.

---

## README Addition

A new "Koan 3 – Aeron Cluster" section covering:
- Four-terminal run sequence
- Failover walkthrough (kill leader, watch re-election)
- Snapshot restore walkthrough (kill all, restart, verify state)
