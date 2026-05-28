package io.aeron.koans.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.koans.sbe.*;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
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
        final ConcurrentLinkedQueue<String> commandQueue = new ConcurrentLinkedQueue<>();

        final Thread stdinThread = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    final String trimmed = line.trim();
                    if ("quit".equalsIgnoreCase(trimmed)) {
                        running.set(false);
                        break;
                    }
                    if (!trimmed.isEmpty()) commandQueue.offer(trimmed);
                }
            } catch (final Exception ignored) {
                running.set(false);
            }
        }, "stdin");
        stdinThread.setDaemon(true);
        stdinThread.start();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (
            MediaDriver driver = MediaDriver.launch(driverCtx);
            AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp")
                    .ingressEndpoints(ClusterNode.INGRESS_ENDPOINTS)
                    .egressChannel(EGRESS_CHANNEL)
                    .egressStreamId(EGRESS_STREAM)
                    .egressListener(new ClientEgressListener()))
        ) {
            System.out.println("[Client] Connected. Commands: inc|dec|reset|get|del <key> [delta], quit");

            while (running.get()) {
                cluster.pollEgress();
                final String cmd = commandQueue.poll();
                if (cmd != null) parseAndSend(cmd, cluster, running);
                Thread.sleep(1);
            }
        }

        System.out.println("[Client] Done.");
    }

    // -----------------------------------------------------------------------

    private static final MessageHeaderEncoder   headerEncoder  = new MessageHeaderEncoder();
    private static final ClusterCommandEncoder  commandEncoder = new ClusterCommandEncoder();
    private static final ExpandableArrayBuffer  sendBuffer     = new ExpandableArrayBuffer(256);

    static void parseAndSend(final String line, final AeronCluster cluster,
                             final AtomicBoolean running) {
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
                System.out.println("[Client] Unknown command: " + parts[0]);
                yield null;
            }
        };
        if (type == null) return;

        long delta = 1L;
        if (parts.length >= 3) {
            try {
                delta = Long.parseLong(parts[2]);
            } catch (final NumberFormatException e) {
                System.out.println("[Client] Invalid delta: " + parts[2]);
                return;
            }
        }

        commandEncoder.wrapAndApplyHeader(sendBuffer, 0, headerEncoder)
                .type(type)
                .delta(delta)
                .key(parts[1]);

        long result;
        do {
            result = cluster.offer(sendBuffer, 0, commandEncoder.limit());
            if (result == io.aeron.Publication.CLOSED) {
                System.out.println("[Client] Session closed.");
                running.set(false);
                return;
            }
        } while (result < 0);
    }

    // -----------------------------------------------------------------------

    static final class ClientEgressListener implements EgressListener {

        private final MessageHeaderDecoder   headerDecoder   = new MessageHeaderDecoder();
        private final ClusterResponseDecoder responseDecoder = new ClusterResponseDecoder();

        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final DirectBuffer buffer, final int offset,
                              final int length, final Header header) {
            headerDecoder.wrap(buffer, offset);
            if (headerDecoder.templateId() != ClusterResponseDecoder.TEMPLATE_ID) return;

            responseDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(), headerDecoder.version());

            final ResponseStatus status = responseDecoder.status();
            final String         key    = responseDecoder.key();
            final long           value  = responseDecoder.value();

            if (status == ResponseStatus.OK) {
                System.out.printf("[OK] %s = %d%n", key, value);
            } else {
                final String error = responseDecoder.error();
                System.out.printf("[ERROR] %s: %s%n", status, error.isEmpty() ? key : error);
            }
        }

        @Override
        public void onNewLeader(final long clusterSessionId, final long leadershipTermId,
                                final int leaderMemberId, final String ingressEndpoints) {
            System.out.println("[Leader] node=" + leaderMemberId);
        }

        @Override
        public void onSessionEvent(final long correlationId, final long clusterSessionId,
                                   final long leadershipTermId, final int leaderMemberId,
                                   final EventCode code, final String detail) {
            if (code != EventCode.OK) {
                System.out.println("[Session] event=" + code + " detail=" + detail);
            }
        }
    }
}
