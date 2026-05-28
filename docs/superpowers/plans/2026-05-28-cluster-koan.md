# Koan 3 – Aeron Cluster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Koan 3 to the aeron-koans project: a 3-node Raft cluster hosting a replicated named-counter map with snapshots, failover, and full unhappy-path coverage.

**Architecture:** A single parameterised `ClusterNode` class launches as three separate JVM processes (args `0`, `1`, `2`). A `CounterMapService` implements `ClusteredService` and owns all state-machine and snapshot logic. A `ClusterClient` connects via Aeron ingress, sends SBE-encoded commands from stdin, and prints SBE-encoded responses.

**Tech Stack:** Aeron Cluster 1.44.0 (bundled in `aeron-all`), SBE 1.30.0, Agrona, Java 17, JUnit 5.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `build.gradle.kts` | Add JUnit 5 test dep, `test { useJUnitPlatform() }`, four run tasks |
| Modify | `src/main/resources/sbe/messages.xml` | Add `CommandType`, `ResponseStatus` enums and messages 5–8 |
| Create | `src/main/java/io/aeron/koans/cluster/Result.java` | Package-private record: `(ResponseStatus, long, String)` |
| Create | `src/main/java/io/aeron/koans/cluster/CounterMapService.java` | `ClusteredService` — state machine, snapshot encode/load |
| Create | `src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java` | Unit tests for state machine and snapshot round-trip |
| Create | `src/main/java/io/aeron/koans/cluster/ClusterNode.java` | Entry point; configures and launches each cluster node |
| Create | `src/main/java/io/aeron/koans/cluster/ClusterClient.java` | Interactive stdin client with egress listener |
| Modify | `README.md` | Add Koan 3 section |

**Port layout** (5 ports per node, stride 100):

| Node | Ingress | Consensus | Log   | Catchup | Archive |
|------|---------|-----------|-------|---------|---------|
| 0    | 20000   | 20001     | 20002 | 20003   | 20004   |
| 1    | 20100   | 20101     | 20102 | 20103   | 20104   |
| 2    | 20200   | 20201     | 20202 | 20203   | 20204   |

---

## Task 1: Enable JUnit 5

**Files:**
- Modify: `build.gradle.kts`

- [ ] **Step 1: Add test dependency and runner config**

In `build.gradle.kts`, add inside `dependencies { ... }`:

```kotlin
testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
testRuntimeOnly("org.junit.platform:junit-platform-launcher")
```

And add after the existing `tasks.named("compileJava")` block:

```kotlin
tasks.test {
    useJUnitPlatform()
}
```

- [ ] **Step 2: Verify build still compiles**

```bash
./gradlew build -x test
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add build.gradle.kts
git commit -m "build: add JUnit 5 test dependency"
```

---

## Task 2: Add SBE Schema Entries

**Files:**
- Modify: `src/main/resources/sbe/messages.xml`

- [ ] **Step 1: Add new enums inside `<types>`**

In `src/main/resources/sbe/messages.xml`, insert after the `OrdStatus` enum (before the closing `</types>` tag):

```xml
    <!-- Cluster koan: command type -->
    <enum name="CommandType" encodingType="uint8">
        <validValue name="INCREMENT">0</validValue>
        <validValue name="DECREMENT">1</validValue>
        <validValue name="RESET">2</validValue>
        <validValue name="GET">3</validValue>
        <validValue name="DELETE">4</validValue>
    </enum>

    <!-- Cluster koan: response status -->
    <enum name="ResponseStatus" encodingType="uint8">
        <validValue name="OK">0</validValue>
        <validValue name="KEY_NOT_FOUND">1</validValue>
        <validValue name="UNDERFLOW">2</validValue>
        <validValue name="OVERFLOW">3</validValue>
        <validValue name="INVALID_DELTA">4</validValue>
    </enum>
```

- [ ] **Step 2: Add new messages at the bottom of the file (before `</sbe:messageSchema>`)**

```xml
    <!-- ===================================================================
         Aeron Cluster: ClusterCommand (template id=5)
         =================================================================== -->
    <sbe:message name="ClusterCommand" id="5" description="Named-counter cluster command">
        <field name="type"  id="1" type="CommandType"/>
        <field name="delta" id="2" type="int64"/>
        <data  name="key"   id="3" type="varAsciiEncoding"/>
    </sbe:message>

    <!-- ===================================================================
         Aeron Cluster: ClusterResponse (template id=6)
         =================================================================== -->
    <sbe:message name="ClusterResponse" id="6" description="Named-counter cluster response">
        <field name="status" id="1" type="ResponseStatus"/>
        <field name="value"  id="2" type="int64"/>
        <data  name="key"    id="3" type="varAsciiEncoding"/>
        <data  name="error"  id="4" type="varAsciiEncoding"/>
    </sbe:message>

    <!-- ===================================================================
         Aeron Cluster: SnapshotHeader (template id=7)
         =================================================================== -->
    <sbe:message name="SnapshotHeader" id="7" description="Number of entries in the snapshot">
        <field name="count" id="1" type="int32"/>
    </sbe:message>

    <!-- ===================================================================
         Aeron Cluster: SnapshotEntry (template id=8)
         =================================================================== -->
    <sbe:message name="SnapshotEntry" id="8" description="One named-counter snapshot entry">
        <field name="value" id="1" type="int64"/>
        <data  name="key"   id="2" type="varAsciiEncoding"/>
    </sbe:message>
```

- [ ] **Step 3: Generate SBE codecs and verify**

```bash
./gradlew generateSbeSources
```

Expected: `BUILD SUCCESSFUL` and new files appear under `build/generated/sbe/main/java/io/aeron/koans/sbe/`:
`ClusterCommandEncoder.java`, `ClusterCommandDecoder.java`, `ClusterResponseEncoder.java`, `ClusterResponseDecoder.java`, `SnapshotHeaderEncoder.java`, `SnapshotHeaderDecoder.java`, `SnapshotEntryEncoder.java`, `SnapshotEntryDecoder.java`, `CommandType.java`, `ResponseStatus.java`

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/sbe/messages.xml
git commit -m "sbe: add ClusterCommand, ClusterResponse, SnapshotHeader, SnapshotEntry schemas"
```

---

## Task 3: Result Record

**Files:**
- Create: `src/main/java/io/aeron/koans/cluster/Result.java`

- [ ] **Step 1: Create the file**

```java
package io.aeron.koans.cluster;

import io.aeron.koans.sbe.ResponseStatus;

record Result(ResponseStatus status, long value, String error) {}
```

- [ ] **Step 2: Compile check**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`

---

## Task 4: CounterMapService – State Machine (TDD)

**Files:**
- Create: `src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java`
- Create: `src/main/java/io/aeron/koans/cluster/CounterMapService.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java`:

```java
package io.aeron.koans.cluster;

import io.aeron.koans.sbe.CommandType;
import io.aeron.koans.sbe.ResponseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CounterMapServiceTest {

    private CounterMapService service;

    @BeforeEach
    void setUp() {
        service = new CounterMapService();
    }

    // --- INCREMENT ---

    @Test
    void increment_onNewKey_autoCreatesAtDeltaValue() {
        Result r = service.apply(CommandType.INCREMENT, 5, "hits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(5L, r.value());
    }

    @Test
    void increment_onExistingKey_accumulatesValue() {
        service.apply(CommandType.INCREMENT, 3, "hits");
        Result r = service.apply(CommandType.INCREMENT, 2, "hits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(5L, r.value());
    }

    @Test
    void increment_wouldOverflowLongMaxValue_returnsOverflow() {
        service.apply(CommandType.INCREMENT, Long.MAX_VALUE, "hits");
        Result r = service.apply(CommandType.INCREMENT, 1, "hits");
        assertEquals(ResponseStatus.OVERFLOW, r.status());
    }

    @Test
    void increment_zeroDelta_returnsInvalidDelta() {
        Result r = service.apply(CommandType.INCREMENT, 0, "hits");
        assertEquals(ResponseStatus.INVALID_DELTA, r.status());
    }

    @Test
    void increment_negativeDelta_returnsInvalidDelta() {
        Result r = service.apply(CommandType.INCREMENT, -1, "hits");
        assertEquals(ResponseStatus.INVALID_DELTA, r.status());
    }

    // --- DECREMENT ---

    @Test
    void decrement_onUnknownKey_returnsKeyNotFound() {
        Result r = service.apply(CommandType.DECREMENT, 1, "missing");
        assertEquals(ResponseStatus.KEY_NOT_FOUND, r.status());
    }

    @Test
    void decrement_onExistingKey_subtractsDelta() {
        service.apply(CommandType.INCREMENT, 10, "hits");
        Result r = service.apply(CommandType.DECREMENT, 3, "hits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(7L, r.value());
    }

    @Test
    void decrement_wouldGoBelowLongMinValue_returnsUnderflow() {
        service.apply(CommandType.INCREMENT, 1, "hits"); // hits = 1
        Result r = service.apply(CommandType.DECREMENT, Long.MAX_VALUE, "hits");
        assertEquals(ResponseStatus.UNDERFLOW, r.status());
    }

    @Test
    void decrement_zeroDelta_returnsInvalidDelta() {
        service.apply(CommandType.INCREMENT, 5, "hits");
        Result r = service.apply(CommandType.DECREMENT, 0, "hits");
        assertEquals(ResponseStatus.INVALID_DELTA, r.status());
    }

    // --- RESET ---

    @Test
    void reset_onUnknownKey_returnsKeyNotFound() {
        Result r = service.apply(CommandType.RESET, 1, "missing");
        assertEquals(ResponseStatus.KEY_NOT_FOUND, r.status());
    }

    @Test
    void reset_onExistingKey_setsValueToZero() {
        service.apply(CommandType.INCREMENT, 42, "hits");
        Result r = service.apply(CommandType.RESET, 1, "hits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(0L, r.value());
    }

    // --- GET ---

    @Test
    void get_onUnknownKey_returnsKeyNotFound() {
        Result r = service.apply(CommandType.GET, 1, "missing");
        assertEquals(ResponseStatus.KEY_NOT_FOUND, r.status());
    }

    @Test
    void get_onExistingKey_returnsCurrentValue() {
        service.apply(CommandType.INCREMENT, 7, "visits");
        Result r = service.apply(CommandType.GET, 1, "visits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(7L, r.value());
    }

    // --- DELETE ---

    @Test
    void delete_onUnknownKey_returnsKeyNotFound() {
        Result r = service.apply(CommandType.DELETE, 1, "missing");
        assertEquals(ResponseStatus.KEY_NOT_FOUND, r.status());
    }

    @Test
    void delete_onExistingKey_removesEntryAndReturnsLastValue() {
        service.apply(CommandType.INCREMENT, 5, "visits");
        Result r = service.apply(CommandType.DELETE, 1, "visits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(5L, r.value());
        assertEquals(ResponseStatus.KEY_NOT_FOUND, service.apply(CommandType.GET, 1, "visits").status());
    }
}
```

- [ ] **Step 2: Run to verify tests fail (class not found)**

```bash
./gradlew test --tests "io.aeron.koans.cluster.CounterMapServiceTest" 2>&1 | tail -20
```

Expected: compilation error — `CounterMapService` does not exist yet.

- [ ] **Step 3: Create CounterMapService with state machine**

Create `src/main/java/io/aeron/koans/cluster/CounterMapService.java`:

```java
package io.aeron.koans.cluster;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.koans.sbe.ClusterCommandDecoder;
import io.aeron.koans.sbe.ClusterResponseEncoder;
import io.aeron.koans.sbe.CommandType;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.MessageHeaderEncoder;
import io.aeron.koans.sbe.ResponseStatus;
import io.aeron.koans.sbe.SnapshotEntryDecoder;
import io.aeron.koans.sbe.SnapshotEntryEncoder;
import io.aeron.koans.sbe.SnapshotHeaderDecoder;
import io.aeron.koans.sbe.SnapshotHeaderEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CounterMapService implements ClusteredService {

    private Cluster cluster;
    private final HashMap<String, Long> counters = new HashMap<>();

    private final MessageHeaderDecoder  headerDecoder        = new MessageHeaderDecoder();
    private final MessageHeaderEncoder  headerEncoder        = new MessageHeaderEncoder();
    private final ClusterCommandDecoder commandDecoder       = new ClusterCommandDecoder();
    private final ClusterResponseEncoder responseEncoder     = new ClusterResponseEncoder();
    private final SnapshotHeaderEncoder snapshotHeaderEncoder = new SnapshotHeaderEncoder();
    private final SnapshotHeaderDecoder snapshotHeaderDecoder = new SnapshotHeaderDecoder();
    private final SnapshotEntryEncoder  snapshotEntryEncoder  = new SnapshotEntryEncoder();
    private final SnapshotEntryDecoder  snapshotEntryDecoder  = new SnapshotEntryDecoder();
    private final ExpandableArrayBuffer responseBuffer       = new ExpandableArrayBuffer(512);

    // -----------------------------------------------------------------------
    // ClusteredService lifecycle
    // -----------------------------------------------------------------------

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        System.out.println("[Service] Session opened: " + session.id());
    }

    @Override
    public void onSessionClose(
            final ClientSession session,
            final long timestamp,
            final io.aeron.cluster.service.CloseReason closeReason) {
        System.out.println("[Service] Session closed: " + session.id() + " reason=" + closeReason);
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        headerDecoder.wrap(buffer, offset);
        commandDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        final CommandType type  = commandDecoder.type();
        final long        delta = commandDecoder.delta();
        final String      key   = commandDecoder.key();

        final Result result = apply(type, delta, key);

        responseEncoder.wrapAndApplyHeader(responseBuffer, 0, headerEncoder)
                .status(result.status())
                .value(result.value())
                .key(key)
                .error(result.error());

        long offerResult;
        do {
            offerResult = session.offer(responseBuffer, 0, responseEncoder.limit());
            if (offerResult == Publication.CLOSED) {
                return;
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {}

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        System.out.println("[Service] Role changed to: " + newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster) {}

    // -----------------------------------------------------------------------
    // State machine — package-private for unit testing
    // -----------------------------------------------------------------------

    Result apply(final CommandType type, final long delta, final String key) {
        return switch (type) {
            case INCREMENT -> applyIncrement(key, delta);
            case DECREMENT -> applyDecrement(key, delta);
            case RESET     -> applyReset(key);
            case GET       -> applyGet(key);
            case DELETE    -> applyDelete(key);
            default        -> new Result(ResponseStatus.INVALID_DELTA, 0, "unknown command type");
        };
    }

    private Result applyIncrement(final String key, final long delta) {
        if (delta <= 0) {
            return new Result(ResponseStatus.INVALID_DELTA, 0, "delta must be > 0");
        }
        final long current = counters.getOrDefault(key, 0L);
        if (Long.MAX_VALUE - current < delta) {
            return new Result(ResponseStatus.OVERFLOW, current, "increment would overflow Long.MAX_VALUE");
        }
        final long next = current + delta;
        counters.put(key, next);
        return new Result(ResponseStatus.OK, next, "");
    }

    private Result applyDecrement(final String key, final long delta) {
        if (delta <= 0) {
            return new Result(ResponseStatus.INVALID_DELTA, 0, "delta must be > 0");
        }
        if (!counters.containsKey(key)) {
            return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        }
        final long current = counters.get(key);
        if (current - Long.MIN_VALUE < delta) {
            return new Result(ResponseStatus.UNDERFLOW, current, "decrement would underflow Long.MIN_VALUE");
        }
        final long next = current - delta;
        counters.put(key, next);
        return new Result(ResponseStatus.OK, next, "");
    }

    private Result applyReset(final String key) {
        if (!counters.containsKey(key)) {
            return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        }
        counters.put(key, 0L);
        return new Result(ResponseStatus.OK, 0L, "");
    }

    private Result applyGet(final String key) {
        if (!counters.containsKey(key)) {
            return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        }
        return new Result(ResponseStatus.OK, counters.get(key), "");
    }

    private Result applyDelete(final String key) {
        if (!counters.containsKey(key)) {
            return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        }
        return new Result(ResponseStatus.OK, counters.remove(key), "");
    }

    // -----------------------------------------------------------------------
    // Snapshot — encode/load helpers are package-private for unit testing
    // -----------------------------------------------------------------------

    @Override
    public void onTakeSnapshot(final Publication snapshotPublication) {
        for (final byte[] encoded : encodeSnapshot()) {
            final UnsafeBuffer buf = new UnsafeBuffer(encoded);
            long result;
            do {
                result = snapshotPublication.offer(buf, 0, encoded.length);
            } while (result < 0 && result != Publication.CLOSED);
        }
        System.out.println("[Service] Snapshot taken: " + counters.size() + " entries");
    }

    private void loadSnapshot(final Image snapshotImage) {
        final List<byte[]> buffers = new ArrayList<>();
        while (!snapshotImage.isEndOfStream()) {
            snapshotImage.poll((buffer, offset, length, header) -> {
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                buffers.add(bytes);
            }, 10);
        }
        loadFromEncodedSnapshot(buffers);
        System.out.println("[Service] Snapshot loaded: " + counters.size() + " entries");
    }

    List<byte[]> encodeSnapshot() {
        final List<byte[]> result = new ArrayList<>();

        final ExpandableArrayBuffer headerBuf = new ExpandableArrayBuffer(32);
        snapshotHeaderEncoder.wrapAndApplyHeader(headerBuf, 0, headerEncoder).count(counters.size());
        result.add(Arrays.copyOf(headerBuf.byteArray(), snapshotHeaderEncoder.limit()));

        for (final Map.Entry<String, Long> entry : counters.entrySet()) {
            final ExpandableArrayBuffer entryBuf = new ExpandableArrayBuffer(128);
            snapshotEntryEncoder.wrapAndApplyHeader(entryBuf, 0, headerEncoder)
                    .value(entry.getValue())
                    .key(entry.getKey());
            result.add(Arrays.copyOf(entryBuf.byteArray(), snapshotEntryEncoder.limit()));
        }
        return result;
    }

    void loadFromEncodedSnapshot(final List<byte[]> buffers) {
        counters.clear();
        if (buffers.isEmpty()) {
            return;
        }
        final UnsafeBuffer headerBuf = new UnsafeBuffer(buffers.get(0));
        headerDecoder.wrap(headerBuf, 0);
        snapshotHeaderDecoder.wrap(
                headerBuf,
                MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final int count = snapshotHeaderDecoder.count();

        for (int i = 0; i < count && (i + 1) < buffers.size(); i++) {
            final UnsafeBuffer entryBuf = new UnsafeBuffer(buffers.get(i + 1));
            headerDecoder.wrap(entryBuf, 0);
            snapshotEntryDecoder.wrap(
                    entryBuf,
                    MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());
            counters.put(snapshotEntryDecoder.key(), snapshotEntryDecoder.value());
        }
    }
}
```

- [ ] **Step 4: Run tests and verify they pass**

```bash
./gradlew test --tests "io.aeron.koans.cluster.CounterMapServiceTest" 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`, 18 tests passing, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/aeron/koans/cluster/Result.java \
        src/main/java/io/aeron/koans/cluster/CounterMapService.java \
        src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java
git commit -m "feat: add CounterMapService state machine with full test coverage"
```

---

## Task 5: CounterMapService – Snapshot Tests (TDD)

**Files:**
- Modify: `src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java`

- [ ] **Step 1: Add snapshot tests to the existing test class**

Append these tests inside `CounterMapServiceTest`:

```java
    // --- SNAPSHOT ---

    @Test
    void snapshot_emptyMap_roundTrips() {
        java.util.List<byte[]> snapshot = service.encodeSnapshot();
        CounterMapService restored = new CounterMapService();
        restored.loadFromEncodedSnapshot(snapshot);
        assertEquals(ResponseStatus.KEY_NOT_FOUND, restored.apply(CommandType.GET, 1, "x").status());
    }

    @Test
    void snapshot_populatedMap_roundTrips() {
        service.apply(CommandType.INCREMENT, 10, "hits");
        service.apply(CommandType.INCREMENT, 5,  "visits");
        service.apply(CommandType.INCREMENT, 99, "errors");

        java.util.List<byte[]> snapshot = service.encodeSnapshot();

        CounterMapService restored = new CounterMapService();
        restored.loadFromEncodedSnapshot(snapshot);

        assertEquals(10L, restored.apply(CommandType.GET, 1, "hits").value());
        assertEquals(5L,  restored.apply(CommandType.GET, 1, "visits").value());
        assertEquals(99L, restored.apply(CommandType.GET, 1, "errors").value());
        assertEquals(ResponseStatus.KEY_NOT_FOUND, restored.apply(CommandType.GET, 1, "missing").status());
    }

    @Test
    void snapshot_afterDelete_omitsDeletedKey() {
        service.apply(CommandType.INCREMENT, 3, "a");
        service.apply(CommandType.INCREMENT, 7, "b");
        service.apply(CommandType.DELETE, 1, "a");

        java.util.List<byte[]> snapshot = service.encodeSnapshot();
        CounterMapService restored = new CounterMapService();
        restored.loadFromEncodedSnapshot(snapshot);

        assertEquals(ResponseStatus.KEY_NOT_FOUND, restored.apply(CommandType.GET, 1, "a").status());
        assertEquals(7L, restored.apply(CommandType.GET, 1, "b").value());
    }
```

- [ ] **Step 2: Run tests and verify all pass**

```bash
./gradlew test --tests "io.aeron.koans.cluster.CounterMapServiceTest" 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`, 21 tests passing, 0 failures.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/aeron/koans/cluster/CounterMapServiceTest.java
git commit -m "test: add snapshot round-trip tests for CounterMapService"
```

---

## Task 6: ClusterNode

**Files:**
- Create: `src/main/java/io/aeron/koans/cluster/ClusterNode.java`

- [ ] **Step 1: Create ClusterNode.java**

```java
package io.aeron.koans.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.SigInt;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Cluster – Node.
 *
 * <p>Launches one node of a 3-node Raft cluster hosting {@link CounterMapService}.
 * Pass node ID (0, 1, or 2) as {@code args[0]}.
 *
 * <pre>
 *   Node 0: ingress=20000  consensus=20001  log=20002  catchup=20003  archive=20004
 *   Node 1: ingress=20100  consensus=20101  log=20102  catchup=20103  archive=20104
 *   Node 2: ingress=20200  consensus=20201  log=20202  catchup=20203  archive=20204
 * </pre>
 *
 * <p>Start all three nodes before running {@link ClusterClient}.
 */
public class ClusterNode {

    static final String CLUSTER_MEMBERS =
            "0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004|" +
            "1,localhost:20100,localhost:20101,localhost:20102,localhost:20103,localhost:20104|" +
            "2,localhost:20200,localhost:20201,localhost:20202,localhost:20203,localhost:20204";

    static final String INGRESS_ENDPOINTS = "0=localhost:20000,1=localhost:20100,2=localhost:20200";

    public static void main(final String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ClusterNode <nodeId: 0|1|2>");
            System.exit(1);
        }

        final int nodeId = Integer.parseInt(args[0]);
        final int base   = 20000 + nodeId * 100;

        final String aeronDir    = System.getProperty("java.io.tmpdir") + "/aeron-cluster-node-" + nodeId;
        final String archiveDir  = System.getProperty("java.io.tmpdir") + "/aeron-cluster-archive-" + nodeId;
        final String clusterDir  = System.getProperty("java.io.tmpdir") + "/aeron-cluster-consensus-" + nodeId;

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("[Node " + nodeId + "] Starting…");
        System.out.println("[Node " + nodeId + "]   aeron   : " + aeronDir);
        System.out.println("[Node " + nodeId + "]   archive : " + archiveDir);
        System.out.println("[Node " + nodeId + "]   cluster : " + clusterDir);

        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDir(new File(archiveDir))
                .controlChannel("aeron:udp?endpoint=localhost:" + (base + 4))
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .deleteArchiveOnStart(true);

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(clusterDir))
                .clusterMemberId(nodeId)
                .clusterMembers(CLUSTER_MEMBERS)
                .ingressChannel("aeron:udp?endpoint=localhost:" + base)
                .archiveContext(new AeronArchive.Context()
                        .controlRequestChannel("aeron:udp?endpoint=localhost:" + (base + 4))
                        .controlResponseChannel("aeron:udp?endpoint=localhost:" + (base + 5))
                        .aeronDirectoryName(aeronDir))
                .snapshotIntervalNs(30_000_000_000L);

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(clusterDir))
                .clusteredService(new CounterMapService());

        try (ClusteredMediaDriver ignored = ClusteredMediaDriver.launch(
                     mediaDriverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer ignored2 = ClusteredServiceContainer.launch(serviceCtx)) {

            System.out.println("[Node " + nodeId + "] Running (Ctrl-C to quit)");
            while (running.get()) {
                Thread.sleep(100);
            }
        }

        System.out.println("[Node " + nodeId + "] Done");
    }
}
```

- [ ] **Step 2: Compile check**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add src/main/java/io/aeron/koans/cluster/ClusterNode.java
git commit -m "feat: add ClusterNode launcher for 3-node Aeron Cluster"
```

---

## Task 7: ClusterClient

**Files:**
- Create: `src/main/java/io/aeron/koans/cluster/ClusterClient.java`

- [ ] **Step 1: Create ClusterClient.java**

```java
package io.aeron.koans.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.koans.sbe.ClusterCommandEncoder;
import io.aeron.koans.sbe.ClusterResponseDecoder;
import io.aeron.koans.sbe.CommandType;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.MessageHeaderEncoder;
import io.aeron.koans.sbe.ResponseStatus;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Cluster – interactive client.
 *
 * <p>Connects to all three cluster nodes and routes commands to the current leader.
 * Start all three {@link ClusterNode} processes first.
 *
 * <p>Commands: {@code inc|dec|reset|get|del <key> [delta]}, {@code quit}
 */
public class ClusterClient {

    private static final String EGRESS_CHANNEL  = "aeron:udp?endpoint=localhost:19000";
    private static final int    EGRESS_STREAM   = 101;

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        final String aeronDir = System.getProperty("java.io.tmpdir") + "/aeron-cluster-client";

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver ignored = MediaDriver.launch(driverCtx);
             AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
                     .aeronDirectoryName(aeronDir)
                     .ingressChannel("aeron:udp")
                     .ingressEndpoints(ClusterNode.INGRESS_ENDPOINTS)
                     .egressChannel(EGRESS_CHANNEL)
                     .egressStreamId(EGRESS_STREAM)
                     .egressListener(new ClientEgressListener()))) {

            System.out.println("[Client] Connected. Commands: inc|dec|reset|get|del <key> [delta], quit");

            final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while (running.get() && (line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if ("quit".equalsIgnoreCase(trimmed)) {
                    break;
                }
                parseAndSend(trimmed, cluster, running);

                // Poll egress until response arrives (max 5 s)
                final long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && running.get()) {
                    cluster.pollEgress();
                    Thread.sleep(1);
                }
            }
        }

        System.out.println("[Client] Done");
    }

    // -----------------------------------------------------------------------

    private static final MessageHeaderEncoder  headerEncoder  = new MessageHeaderEncoder();
    private static final ClusterCommandEncoder commandEncoder = new ClusterCommandEncoder();
    private static final ExpandableArrayBuffer sendBuffer     = new ExpandableArrayBuffer(256);

    static void parseAndSend(final String line, final AeronCluster cluster, final AtomicBoolean running) {
        final String[] parts = line.split("\\s+");
        if (parts.length < 2) {
            System.out.println("[Client] Usage: inc|dec|reset|get|del <key> [delta]");
            return;
        }

        final CommandType type = switch (parts[0].toLowerCase()) {
            case "inc"   -> CommandType.INCREMENT;
            case "dec"   -> CommandType.DECREMENT;
            case "reset" -> CommandType.RESET;
            case "get"   -> CommandType.GET;
            case "del"   -> CommandType.DELETE;
            default -> {
                System.out.println("[Client] Unknown command: " + parts[0] +
                        ". Use: inc|dec|reset|get|del");
                yield null;
            }
        };

        if (type == null) {
            return;
        }

        long delta = 1;
        if (parts.length >= 3) {
            try {
                delta = Long.parseLong(parts[2]);
            } catch (final NumberFormatException e) {
                System.out.println("[Client] Invalid delta: " + parts[2]);
                return;
            }
        }

        final String key = parts[1];
        commandEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder)
                .type(type)
                .delta(delta)
                .key(key);

        long result;
        do {
            result = cluster.offer(sendBuffer, 0, commandEncoder.limit());
            if (result == io.aeron.Publication.CLOSED) {
                System.out.println("[Client] Session closed");
                running.set(false);
                return;
            }
        } while (result < 0);
    }

    // -----------------------------------------------------------------------

    static class ClientEgressListener implements EgressListener {

        private final MessageHeaderDecoder   headerDecoder   = new MessageHeaderDecoder();
        private final ClusterResponseDecoder responseDecoder = new ClusterResponseDecoder();

        @Override
        public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header) {

            headerDecoder.wrap(buffer, offset);
            responseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

            final ResponseStatus status = responseDecoder.status();
            final String         key    = responseDecoder.key();
            final long           value  = responseDecoder.value();
            final String         error  = responseDecoder.error();

            if (status == ResponseStatus.OK) {
                System.out.printf("[OK] %s = %d%n", key, value);
            } else {
                System.out.printf("[ERROR] %s: %s%n", status, error.isEmpty() ? key : error);
            }
        }

        @Override
        public void onNewLeader(
                final long clusterSessionId,
                final long leaderMemberId,
                final String ingressEndpoints) {
            System.out.println("[Leader] node=" + leaderMemberId);
        }
    }
}
```

- [ ] **Step 2: Compile check**

```bash
./gradlew compileJava
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add src/main/java/io/aeron/koans/cluster/ClusterClient.java
git commit -m "feat: add ClusterClient interactive stdin client"
```

---

## Task 8: Gradle Run Tasks + README

**Files:**
- Modify: `build.gradle.kts`
- Modify: `README.md`

- [ ] **Step 1: Add four run tasks to build.gradle.kts**

After the last `runTask(...)` call (line ending with `"Run the Archive Reader..."`), append:

```kotlin
runTask("runClusterNode0",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 0  (nodeId=0, ingress=20000)")
runTask("runClusterNode1",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 1  (nodeId=1, ingress=20100)")
runTask("runClusterNode2",   "io.aeron.koans.cluster.ClusterNode",
        "Run Cluster Node 2  (nodeId=2, ingress=20200)")
runTask("runClusterClient",  "io.aeron.koans.cluster.ClusterClient",
        "Run Cluster Client  (interactive named-counter REPL)")
```

Then configure each node task to pass its ID as an argument. After the `runTask` calls, add:

```kotlin
tasks.named<JavaExec>("runClusterNode0") { args = listOf("0") }
tasks.named<JavaExec>("runClusterNode1") { args = listOf("1") }
tasks.named<JavaExec>("runClusterNode2") { args = listOf("2") }
```

- [ ] **Step 2: Add Koan 3 section to README.md**

In `README.md`, insert the following after the existing "Koan 2 – Aeron Archive" section (before the "## SBE Schema" heading):

```markdown
---

## Koan 3 – Aeron Cluster (Replicated Named-Counter Map)

Three cluster nodes form a Raft consensus group hosting a replicated `HashMap<String, Long>`.
A client sends named-counter commands (INCREMENT, DECREMENT, RESET, GET, DELETE) and receives
responses. Demonstrates ingress/egress, leader election, unhappy paths, snapshots, and failover.

* **ClusterNode** – one class, three processes (args `0`, `1`, `2`). Runs an embedded
  `ClusteredMediaDriver` (MediaDriver + Archive + ConsensusModule) and a
  `ClusteredServiceContainer` hosting `CounterMapService`.
* **CounterMapService** – implements `ClusteredService`. All state-machine logic, snapshot
  serialisation, and snapshot restore live here.
* **ClusterClient** – interactive REPL. Reads commands from stdin, sends SBE-encoded
  `ClusterCommand` messages, prints SBE-encoded `ClusterResponse` replies.

### Run

Open **four** terminals in order:

```bash
# Terminal 1
./gradlew runClusterNode0

# Terminal 2
./gradlew runClusterNode1

# Terminal 3
./gradlew runClusterNode2

# Terminal 4 – wait until all three nodes print "Running"
./gradlew runClusterClient
```

**Available commands:**

| Command | Effect |
|---------|--------|
| `inc <key> [delta]` | Increment counter (creates at 0 if new) |
| `dec <key> [delta]` | Decrement counter (`KEY_NOT_FOUND` if absent, `UNDERFLOW` if out of range) |
| `reset <key>` | Set counter to 0 (`KEY_NOT_FOUND` if absent) |
| `get <key>` | Read current value |
| `del <key>` | Remove counter, returns last value |
| `quit` | Exit the client |

**Failover walkthrough:**
1. Run some commands and note the values.
2. Press `Ctrl-C` on whichever node the client logged as `[Leader]`.
3. Watch the remaining two nodes elect a new leader (`[Leader] node=N` in the client).
4. Continue sending commands — the cluster operates normally with 2/3 nodes.
5. Restart the stopped node — it catches up from the replicated log.

**Snapshot walkthrough:**
1. Run several commands to populate counters.
2. Wait ~30 seconds (the cluster takes an automatic snapshot).
3. Press `Ctrl-C` on all three node terminals.
4. Restart all three nodes.
5. Connect the client — your counters are restored from the snapshot.
```

- [ ] **Step 3: Full build and test**

```bash
./gradlew build
```

Expected: `BUILD SUCCESSFUL`, all 21 tests pass.

- [ ] **Step 4: Commit**

```bash
git add build.gradle.kts README.md
git commit -m "feat: add Gradle run tasks and README section for Koan 3 Aeron Cluster"
```

---

## Self-Review Checklist

- [x] **SBE schema** — messages 5–8, two new enums: Task 2
- [x] **State machine** — all 5 commands + 5 error cases: Task 4
- [x] **Snapshot encode/load** — covered with 3 tests: Task 5
- [x] **ClusterNode** — parameterised by nodeId, correct port derivation: Task 6
- [x] **ClusterClient** — stdin parsing, egress listener, leader change event: Task 7
- [x] **Gradle tasks** — 4 run tasks with correct args: Task 8
- [x] **README** — run sequence, failover walkthrough, snapshot walkthrough: Task 8
- [x] **No placeholders** — all code blocks are complete
- [x] **Type consistency** — `Result`, `CommandType`, `ResponseStatus`, `CounterMapService.apply()` names are consistent across all tasks
