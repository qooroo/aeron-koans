package io.aeron.koans.archive;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.MessageHeaderEncoder;
import io.aeron.koans.sbe.PingEncoder;
import io.aeron.koans.sbe.PongDecoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Aeron Archive – Client side.
 *
 * <p>Sends {@code Ping} messages to {@link ArchiveServer}, receives {@code Pong} replies
 * (SBE-encoded), and records the incoming Pongs in a local Aeron Archive.
 *
 * <p>After all pings are acknowledged the application waits until interrupted (Ctrl-C)
 * so its archive remains available for {@link ArchiveReader}.
 *
 * <p><b>Network topology</b>
 * <pre>
 *   ArchiveClient  ──[PING udp:20131]──►  ArchiveServer
 *   ArchiveClient  ◄─[PONG udp:20132]──  ArchiveServer (client records Pongs here)
 * </pre>
 */
public class ArchiveClient {

    /** Archive control channel – ArchiveReader connects here. */
    static final String ARCHIVE_CONTROL_CHANNEL          = "aeron:udp?endpoint=localhost:8020";
    static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8021";

    static final String AERON_DIR   = System.getProperty("java.io.tmpdir") + "/aeron-archive-client";
    static final String ARCHIVE_DIR = System.getProperty("java.io.tmpdir") + "/archive-client";

    private static final int NUM_PINGS = 5;

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running       = new AtomicBoolean(true);
        final AtomicInteger pongsReceived = new AtomicInteger(0);
        SigInt.register(() -> running.set(false));

        System.out.println("[Client] Starting archiving media driver...");
        System.out.println("[Client]   aeron dir   : " + AERON_DIR);
        System.out.println("[Client]   archive dir : " + ARCHIVE_DIR);
        System.out.println("[Client]   control     : " + ARCHIVE_CONTROL_CHANNEL);

        final ArchivingMediaDriver archivingDriver = ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(AERON_DIR)
                        .dirDeleteOnStart(true),
                new Archive.Context()
                        .aeronDirectoryName(AERON_DIR)
                        .archiveDir(new File(ARCHIVE_DIR))
                        .controlChannel(ARCHIVE_CONTROL_CHANNEL)
                        .replicationChannel("aeron:udp?endpoint=localhost:0")
                        .deleteArchiveOnStart(true));

        try (archivingDriver;
             AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                     .aeronDirectoryName(AERON_DIR)
                     .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
                     .controlResponseChannel(ARCHIVE_CONTROL_RESPONSE_CHANNEL))) {

            final Aeron aeron = aeronArchive.context().aeron();

            // Tell the archive to record Pong messages arriving from the server
            aeronArchive.startRecording(ArchiveServer.PONG_CHANNEL, ArchiveServer.STREAM_ID, SourceLocation.REMOTE);
            System.out.println("[Client] Recording started on " +
                    ArchiveServer.PONG_CHANNEL + " stream=" + ArchiveServer.STREAM_ID);

            try (Publication pingPub = aeron.addPublication(
                         ArchiveServer.PING_CHANNEL, ArchiveServer.STREAM_ID);
                 Subscription pongSub = aeron.addSubscription(
                         ArchiveServer.PONG_CHANNEL, ArchiveServer.STREAM_ID)) {

                System.out.println("[Client] Waiting for Server to connect...");
                while (!pingPub.isConnected() && running.get()) {
                    Thread.sleep(100);
                }

                if (!running.get()) {
                    return;
                }

                System.out.println("[Client] Connected – sending " + NUM_PINGS + " Pings");

                // Send Pings
                for (int i = 0; i < NUM_PINGS && running.get(); i++) {
                    sendPing(pingPub, i, System.nanoTime());
                    System.out.printf("[Client] Sent    Ping  → id=%d%n", i);
                    Thread.sleep(50);
                }

                // Collect Pongs (10-second deadline)
                final long deadline = System.currentTimeMillis() + 10_000;
                while (running.get()
                        && pongsReceived.get() < NUM_PINGS
                        && System.currentTimeMillis() < deadline) {
                    pongSub.poll(
                            (buffer, offset, length, header) ->
                                    onPong(buffer, offset, length, header, pongsReceived),
                            10);
                    Thread.sleep(1);
                }

                System.out.println("[Client] Received " + pongsReceived.get() + "/" + NUM_PINGS + " Pongs");
                System.out.println("[Client] Recordings available in " + ARCHIVE_DIR);
                System.out.println("[Client] Waiting (Ctrl-C to quit – keep alive for ArchiveReader)");

                // Keep the archive alive so ArchiveReader can connect
                while (running.get()) {
                    Thread.sleep(500);
                }
            }
        }

        System.out.println("[Client] Done");
    }

    // -----------------------------------------------------------------------
    // Encoding / decoding helpers
    // -----------------------------------------------------------------------

    static void sendPing(final Publication publication, final long id, final long sendTimeNs) {
        final MessageHeaderEncoder  headerEncoder = new MessageHeaderEncoder();
        final PingEncoder           encoder       = new PingEncoder();
        final ExpandableArrayBuffer buffer        = new ExpandableArrayBuffer(64);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .id(id)
                .sendTimeNs(sendTimeNs);

        long result;
        do {
            result = publication.offer(buffer, 0, encoder.limit());
        } while (result < 0 && result != Publication.CLOSED);
    }

    static void onPong(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header,
            final AtomicInteger pongsReceived) {

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final PongDecoder          pongDecoder   = new PongDecoder();

        headerDecoder.wrap(buffer, offset);
        pongDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        final long id         = pongDecoder.id();
        final long sendTimeNs = pongDecoder.sendTimeNs();
        final long rttNs      = System.nanoTime() - sendTimeNs;

        System.out.printf("[Client] Received Pong  → id=%d RTT=%.3fms%n",
                id, rttNs / 1_000_000.0);
        pongsReceived.incrementAndGet();
    }
}
