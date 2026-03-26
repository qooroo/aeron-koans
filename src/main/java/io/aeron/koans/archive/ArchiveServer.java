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
import io.aeron.koans.sbe.PingDecoder;
import io.aeron.koans.sbe.PongEncoder;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Aeron Archive – Server side.
 *
 * <p>Receives {@code Ping} messages from {@link ArchiveClient} over UDP, records them in a
 * local Aeron Archive, and sends back {@code Pong} replies.
 *
 * <p>Start this process first, then {@link ArchiveClient}, then {@link ArchiveReader}.
 *
 * <p><b>Network topology</b>
 * <pre>
 *   ArchiveClient  ──[PING udp:20131]──►  ArchiveServer (records Pings)
 *   ArchiveClient  ◄─[PONG udp:20132]──  ArchiveServer
 * </pre>
 */
public class ArchiveServer {

    /** Channel on which the Server subscribes for incoming Pings. */
    static final String PING_CHANNEL  = "aeron:udp?endpoint=localhost:20131";

    /** Channel on which the Server publishes Pong replies. */
    static final String PONG_CHANNEL  = "aeron:udp?endpoint=localhost:20132";

    static final int STREAM_ID = 1;

    /** Archive control channel – ArchiveReader connects here. */
    static final String ARCHIVE_CONTROL_CHANNEL          = "aeron:udp?endpoint=localhost:8010";
    static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8011";

    static final String AERON_DIR   = System.getProperty("java.io.tmpdir") + "/aeron-archive-server";
    static final String ARCHIVE_DIR = System.getProperty("java.io.tmpdir") + "/archive-server";

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("[Server] Starting archiving media driver...");
        System.out.println("[Server]   aeron dir   : " + AERON_DIR);
        System.out.println("[Server]   archive dir : " + ARCHIVE_DIR);
        System.out.println("[Server]   control     : " + ARCHIVE_CONTROL_CHANNEL);

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

            // Tell the archive to record every Ping that arrives from the network
            aeronArchive.startRecording(PING_CHANNEL, STREAM_ID, SourceLocation.REMOTE);
            System.out.println("[Server] Recording started on " + PING_CHANNEL + " stream=" + STREAM_ID);

            final AtomicReference<Publication> pongPubRef = new AtomicReference<>();

            try (Subscription pingSub = aeron.addSubscription(PING_CHANNEL, STREAM_ID);
                 Publication  pongPub = aeron.addPublication(PONG_CHANNEL, STREAM_ID)) {

                pongPubRef.set(pongPub);
                System.out.println("[Server] Ready – waiting for Pings (Ctrl-C to quit)");

                while (running.get()) {
                    pingSub.poll(
                            (buffer, offset, length, header) ->
                                    onPing(buffer, offset, length, header, pongPub),
                            10);
                    Thread.sleep(1);
                }
            }
        }

        System.out.println("[Server] Done");
    }

    // -----------------------------------------------------------------------
    // Fragment handler
    // -----------------------------------------------------------------------

    static void onPing(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header,
            final Publication pongPub) {

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        final PingDecoder          pingDecoder   = new PingDecoder();

        headerDecoder.wrap(buffer, offset);
        pingDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        final long id         = pingDecoder.id();
        final long sendTimeNs = pingDecoder.sendTimeNs();

        System.out.printf("[Server] Received Ping  → id=%d sendTimeNs=%d%n", id, sendTimeNs);

        sendPong(pongPub, id, System.nanoTime());
    }

    static void sendPong(final Publication publication, final long id, final long sendTimeNs) {
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final PongEncoder          encoder       = new PongEncoder();
        final ExpandableArrayBuffer buffer       = new ExpandableArrayBuffer(64);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .id(id)
                .sendTimeNs(sendTimeNs);

        long result;
        do {
            result = publication.offer(buffer, 0, encoder.limit());
        } while (result < 0 && result != Publication.CLOSED);

        System.out.printf("[Server] Sent    Pong   → id=%d%n", id);
    }
}
