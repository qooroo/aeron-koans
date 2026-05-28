package io.aeron.koans.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.koans.sbe.*;
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
    final HashMap<String, Long> counters = new HashMap<>();

    private final MessageHeaderDecoder   headerDecoder         = new MessageHeaderDecoder();
    private final MessageHeaderEncoder   headerEncoder         = new MessageHeaderEncoder();
    private final ClusterCommandDecoder  commandDecoder        = new ClusterCommandDecoder();
    private final ClusterResponseEncoder responseEncoder       = new ClusterResponseEncoder();
    private final SnapshotHeaderEncoder  snapshotHeaderEncoder = new SnapshotHeaderEncoder();
    private final SnapshotHeaderDecoder  snapshotHeaderDecoder = new SnapshotHeaderDecoder();
    private final SnapshotEntryEncoder   snapshotEntryEncoder  = new SnapshotEntryEncoder();
    private final SnapshotEntryDecoder   snapshotEntryDecoder  = new SnapshotEntryDecoder();
    private final ExpandableArrayBuffer  responseBuffer        = new ExpandableArrayBuffer(512);

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
    public void onSessionClose(final ClientSession session, final long timestamp,
                               final CloseReason closeReason) {
        System.out.println("[Service] Session closed: " + session.id() + " reason=" + closeReason);
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp,
                                 final DirectBuffer buffer, final int offset,
                                 final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        commandDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(), headerDecoder.version());

        final CommandType type  = commandDecoder.type();
        final long        delta = commandDecoder.delta();
        final String      key   = commandDecoder.key();
        final Result      result = apply(type, delta, key);

        responseEncoder.wrapAndApplyHeader(responseBuffer, 0, headerEncoder)
                .status(result.status())
                .value(result.value())
                .key(key)
                .error(result.error());

        long offerResult;
        do {
            offerResult = session.offer(responseBuffer, 0, responseEncoder.limit());
            if (offerResult == ExclusivePublication.CLOSED) return;
            if (offerResult < 0) cluster.idleStrategy().idle();
        } while (offerResult < 0);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {}

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        System.out.println("[Service] Role → " + newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        System.out.println("[Service] Terminating");
    }

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
        if (delta <= 0) return new Result(ResponseStatus.INVALID_DELTA, 0, "delta must be > 0");
        final long current = counters.getOrDefault(key, 0L);
        try {
            final long next = Math.addExact(current, delta);
            counters.put(key, next);
            return new Result(ResponseStatus.OK, next, "");
        } catch (final ArithmeticException e) {
            return new Result(ResponseStatus.OVERFLOW, current, "increment would overflow");
        }
    }

    private Result applyDecrement(final String key, final long delta) {
        if (delta <= 0) return new Result(ResponseStatus.INVALID_DELTA, 0, "delta must be > 0");
        if (!counters.containsKey(key)) return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        final long current = counters.get(key);
        try {
            final long next = Math.subtractExact(current, delta);
            counters.put(key, next);
            return new Result(ResponseStatus.OK, next, "");
        } catch (final ArithmeticException e) {
            return new Result(ResponseStatus.UNDERFLOW, current, "decrement would underflow");
        }
    }

    private Result applyReset(final String key) {
        if (!counters.containsKey(key)) return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        counters.put(key, 0L);
        return new Result(ResponseStatus.OK, 0L, "");
    }

    private Result applyGet(final String key) {
        if (!counters.containsKey(key)) return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        return new Result(ResponseStatus.OK, counters.get(key), "");
    }

    private Result applyDelete(final String key) {
        if (!counters.containsKey(key)) return new Result(ResponseStatus.KEY_NOT_FOUND, 0, "key not found: " + key);
        return new Result(ResponseStatus.OK, counters.remove(key), "");
    }

    // -----------------------------------------------------------------------
    // Snapshot encode/load — package-private for testing
    // -----------------------------------------------------------------------

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
        if (buffers.isEmpty()) return;

        final UnsafeBuffer headerBuf = new UnsafeBuffer(buffers.get(0));
        headerDecoder.wrap(headerBuf, 0);
        snapshotHeaderDecoder.wrap(headerBuf, MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(), headerDecoder.version());
        final int count = snapshotHeaderDecoder.count();

        for (int i = 0; i < count && (i + 1) < buffers.size(); i++) {
            final UnsafeBuffer entryBuf = new UnsafeBuffer(buffers.get(i + 1));
            headerDecoder.wrap(entryBuf, 0);
            snapshotEntryDecoder.wrap(entryBuf, MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(), headerDecoder.version());
            counters.put(snapshotEntryDecoder.key(), snapshotEntryDecoder.value());
        }
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        for (final byte[] encoded : encodeSnapshot()) {
            final UnsafeBuffer buf = new UnsafeBuffer(encoded);
            long result;
            do {
                result = snapshotPublication.offer(buf, 0, encoded.length);
            } while (result < 0 && result != ExclusivePublication.CLOSED);
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
}
