package io.aeron.koans.messaging;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.koans.sbe.ExecType;
import io.aeron.koans.sbe.ExecutionReportEncoder;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.MessageHeaderEncoder;
import io.aeron.koans.sbe.NewOrderSingleDecoder;
import io.aeron.koans.sbe.OrdStatus;
import io.aeron.koans.sbe.Side;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Aeron Messaging – Venue side.
 *
 * <p>Subscribes for {@code NewOrderSingle} orders (SBE-encoded), decodes each one,
 * and sends back a {@code ExecutionReport} (ExecType=NEW) on the reports channel.
 *
 * <p>Start this process first, then run {@link MessagingClient}.
 */
public class MessagingVenue {

    /** Well-known Aeron directory shared between Venue and Client. */
    static final String AERON_DIR = System.getProperty("java.io.tmpdir") + "/aeron-koans-messaging";

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("[Venue] Starting shared media driver...");

        // Keep a reference to the publication so the fragment handler can use it.
        final AtomicReference<Publication> reportsPubRef = new AtomicReference<>();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(AERON_DIR)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launch(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(AERON_DIR))) {

            try (Subscription ordersSub = aeron.addSubscription(
                         MessagingClient.CHANNEL, MessagingClient.ORDERS_STREAM_ID);
                 Publication reportsPub = aeron.addPublication(
                         MessagingClient.CHANNEL, MessagingClient.REPORTS_STREAM_ID)) {

                reportsPubRef.set(reportsPub);

                System.out.println("[Venue] Listening for orders on " +
                        MessagingClient.CHANNEL + " (Ctrl-C to quit)");

                while (running.get()) {
                    ordersSub.poll(
                            (buffer, offset, length, header) ->
                                    onNewOrderSingle(buffer, offset, length, header, reportsPub),
                            10);
                    Thread.sleep(1);
                }
            }
        }

        System.out.println("[Venue] Done");
    }

    // -----------------------------------------------------------------------
    // Decoding / Encoding helpers
    // -----------------------------------------------------------------------

    static void onNewOrderSingle(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header,
            final Publication reportsPub) {

        final MessageHeaderDecoder   headerDecoder = new MessageHeaderDecoder();
        final NewOrderSingleDecoder  decoder       = new NewOrderSingleDecoder();

        headerDecoder.wrap(buffer, offset);
        decoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        final String clOrdId  = decoder.clOrdId();
        final String symbol   = decoder.symbol();
        final Side   side     = decoder.side();
        final double orderQty = decoder.orderQty();
        final double price    = decoder.price();

        System.out.printf(
                "[Venue] Received NewOrderSingle → clOrdId=%s symbol=%s side=%s " +
                "ordType=%s qty=%.2f price=%.2f%n",
                clOrdId, symbol, side, decoder.ordType(), orderQty, price);

        sendExecutionReport(reportsPub, clOrdId, symbol, side, orderQty);
    }

    static void sendExecutionReport(
            final Publication publication,
            final String clOrdId,
            final String symbol,
            final Side side,
            final double orderQty) {

        final MessageHeaderEncoder   headerEncoder = new MessageHeaderEncoder();
        final ExecutionReportEncoder encoder       = new ExecutionReportEncoder();
        final ExpandableArrayBuffer  buffer        = new ExpandableArrayBuffer(512);

        final String orderId = "ORD-" + System.nanoTime();
        final String execId  = "EXC-" + System.nanoTime();

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .execType(ExecType.NEW)
                .ordStatus(OrdStatus.NEW)
                .side(side)
                .leavesQty(orderQty)
                .cumQty(0.0)
                .avgPx(0.0)
                .orderId(orderId)
                .clOrdId(clOrdId)
                .execId(execId)
                .symbol(symbol);

        long result;
        do {
            result = publication.offer(buffer, 0, encoder.limit());
        } while (result < 0 && result != Publication.CLOSED);

        System.out.printf(
                "[Venue] Sent ExecutionReport → orderId=%s clOrdId=%s execType=NEW ordStatus=NEW%n",
                orderId, clOrdId);
    }
}
