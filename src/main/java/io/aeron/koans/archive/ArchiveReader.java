package io.aeron.koans.archive;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.PingDecoder;
import io.aeron.koans.sbe.PongDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SigInt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Archive – Reader.
 *
 * <p>Connects to a running archive (either the Server's or the Client's), lists all
 * recordings, and replays each one – decoding the SBE messages and printing them to
 * standard output.
 *
 * <p><b>Usage</b>
 * <pre>
 *   # Read from the Server archive (default):
 *   ./gradlew runArchiveReader
 *
 *   # Read from the Client archive:
 *   ./gradlew runArchiveReader --args="client"
 * </pre>
 *
 * <p>Either {@link ArchiveServer} or {@link ArchiveClient} (or both) must be running
 * before this application is started so that their control channels are available.
 */
public class ArchiveReader {

    /** Channel on which this reader receives replayed messages from the archive. */
    static final String REPLAY_CHANNEL   = "aeron:udp?endpoint=localhost:8030";
    static final int    REPLAY_STREAM_ID = 100;

    static final String AERON_DIR = System.getProperty("java.io.tmpdir") + "/aeron-archive-reader";

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        // Select target archive from command-line argument (default: server)
        final String target = (args.length > 0) ? args[0].toLowerCase() : "server";
        final String archiveControlChannel;
        // The Reader uses its own dedicated response port so it doesn't clash with
        // the Server's or Client's existing archive response listeners.
        final String readerResponseChannel;

        if ("client".equals(target)) {
            archiveControlChannel = ArchiveClient.ARCHIVE_CONTROL_CHANNEL;
            readerResponseChannel = "aeron:udp?endpoint=localhost:8051";
            System.out.println("[Reader] Connecting to CLIENT archive at " + archiveControlChannel);
        } else {
            archiveControlChannel = ArchiveServer.ARCHIVE_CONTROL_CHANNEL;
            readerResponseChannel = "aeron:udp?endpoint=localhost:8050";
            System.out.println("[Reader] Connecting to SERVER archive at " + archiveControlChannel);
        }

        // The reader has its own embedded media driver (no archive of its own)
        try (MediaDriver driver = MediaDriver.launchEmbedded();
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                     .aeron(aeron)
                     .controlRequestChannel(archiveControlChannel)
                     .controlResponseChannel(readerResponseChannel))) {

            // Enumerate all recordings in the archive
            final List<long[]> recordings = new ArrayList<>(); // [recordingId, startPos, rawStopPos]

            final int found = aeronArchive.listRecordings(0, Integer.MAX_VALUE,
                    (controlSessionId, correlationId, recordingId,
                     startTimestamp, stopTimestamp,
                     startPosition, stopPosition,
                     initialTermId, segmentFileLength, termBufferLength, mtuLength,
                     sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {

                        System.out.printf(
                                "[Reader] Found recording → id=%d channel=%s streamId=%d " +
                                "startPos=%d stopPos=%d%s%n",
                                recordingId, strippedChannel, streamId,
                                startPosition, stopPosition,
                                stopPosition < 0 ? " (active)" : "");
                        recordings.add(new long[]{recordingId, startPosition, stopPosition});
                    });

            // Resolve active-recording positions OUTSIDE the callback (re-entrant calls are not permitted)
            for (final long[] rec : recordings) {
                if (rec[2] < 0) {
                    rec[2] = aeronArchive.getRecordingPosition(rec[0]);
                }
            }

            if (found == 0) {
                System.out.println("[Reader] No recordings found – ensure ArchiveServer/ArchiveClient has run.");
                return;
            }

            // Replay each recording and decode the SBE messages
            for (final long[] rec : recordings) {
                final long recordingId = rec[0];
                final long startPos    = rec[1];
                final long stopPos     = rec[2];
                final long replayLen   = stopPos - startPos;

                if (replayLen <= 0) {
                    System.out.printf("[Reader] Skipping empty recording id=%d%n", recordingId);
                    continue;
                }

                System.out.printf("[Reader] Replaying recording id=%d length=%d bytes%n",
                        recordingId, replayLen);

                replayRecording(aeronArchive, aeron, recordingId, startPos, replayLen, running);
            }
        }

        System.out.println("[Reader] Done");
    }

    // -----------------------------------------------------------------------
    // Replay helpers
    // -----------------------------------------------------------------------

    static void replayRecording(
            final AeronArchive aeronArchive,
            final Aeron aeron,
            final long recordingId,
            final long startPosition,
            final long replayLength,
            final AtomicBoolean running) throws Exception {

        // Ask the archive to replay the recording onto our replay channel
        aeronArchive.startReplay(recordingId, startPosition, replayLength, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        try (Subscription replaySub = aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID)) {

            // Wait for the replay subscription to connect
            final long connectDeadline = System.currentTimeMillis() + 5_000;
            while (!replaySub.isConnected() && running.get()
                    && System.currentTimeMillis() < connectDeadline) {
                Thread.sleep(10);
            }

            if (!replaySub.isConnected()) {
                System.out.println("[Reader] Timed out waiting for replay to connect");
                return;
            }

            // Drain the replay (5-second window after last message)
            long lastFragment = System.currentTimeMillis();
            while (running.get() && (System.currentTimeMillis() - lastFragment) < 2_000) {
                final int fragments = replaySub.poll(ArchiveReader::decodeAndLog, 10);
                if (fragments > 0) {
                    lastFragment = System.currentTimeMillis();
                } else {
                    Thread.sleep(5);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // SBE decoding
    // -----------------------------------------------------------------------

    static void decodeAndLog(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final io.aeron.logbuffer.Header header) {

        final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch (templateId) {
            case PingDecoder.TEMPLATE_ID -> {
                final PingDecoder pingDecoder = new PingDecoder();
                pingDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        headerDecoder.blockLength(),
                        headerDecoder.version());
                System.out.printf("[Reader] Decoded Ping → id=%d sendTimeNs=%d%n",
                        pingDecoder.id(), pingDecoder.sendTimeNs());
            }
            case PongDecoder.TEMPLATE_ID -> {
                final PongDecoder pongDecoder = new PongDecoder();
                pongDecoder.wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        headerDecoder.blockLength(),
                        headerDecoder.version());
                System.out.printf("[Reader] Decoded Pong → id=%d sendTimeNs=%d%n",
                        pongDecoder.id(), pongDecoder.sendTimeNs());
            }
            default ->
                System.out.printf("[Reader] Unknown templateId=%d length=%d%n", templateId, length);
        }
    }
}
