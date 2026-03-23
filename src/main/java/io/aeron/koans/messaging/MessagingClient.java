package io.aeron.koans.messaging;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.koans.sbe.ExecutionReportDecoder;
import io.aeron.koans.sbe.MessageHeaderDecoder;
import io.aeron.koans.sbe.MessageHeaderEncoder;
import io.aeron.koans.sbe.NewOrderSingleEncoder;
import io.aeron.koans.sbe.OrdType;
import io.aeron.koans.sbe.Side;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Messaging – Client side.
 *
 * <p>Encodes a {@code NewOrderSingle} using SBE and publishes it to the Venue.
 * Waits for the corresponding {@code ExecutionReport} and prints the decoded fields.
 *
 * <p>Run {@link MessagingVenue} first, then this class.
 */
public class MessagingClient {

    /** Channel on which the Venue subscribes for incoming orders. */
    static final String ORDERS_CHANNEL = "aeron:udp?endpoint=localhost:20121";

    /** Channel on which the Client subscribes for execution reports. */
    static final String REPORTS_CHANNEL = "aeron:udp?endpoint=localhost:20122";

    static final int STREAM_ID = 1;

    public static void main(final String[] args) throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("[Client] Starting embedded media driver...");

        try (MediaDriver driver = MediaDriver.launchEmbedded();
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()))) {

            try (Publication ordersPub = aeron.addPublication(ORDERS_CHANNEL, STREAM_ID);
                 Subscription reportsSub = aeron.addSubscription(REPORTS_CHANNEL, STREAM_ID)) {

                System.out.println("[Client] Waiting for Venue to connect on " + ORDERS_CHANNEL + "...");
                while (!ordersPub.isConnected() && running.get()) {
                    Thread.sleep(100);
                }

                if (!running.get()) {
                    return;
                }

                // Encode and send a NewOrderSingle via SBE
                sendNewOrderSingle(ordersPub, "ORD-001", "AAPL", Side.BUY, OrdType.LIMIT, 100.0, 189.50);
                System.out.println("[Client] Sent NewOrderSingle → clOrdId=ORD-001 symbol=AAPL side=BUY qty=100 price=189.50");

                // Poll for ExecutionReport (up to 10 seconds)
                final long deadline = System.currentTimeMillis() + 10_000;
                while (running.get() && System.currentTimeMillis() < deadline) {
                    final int fragments = reportsSub.poll(MessagingClient::onExecutionReport, 10);
                    if (fragments > 0) {
                        break; // received reply – done
                    }
                    Thread.sleep(10);
                }
            }
        }

        System.out.println("[Client] Done");
    }

    // -----------------------------------------------------------------------
    // Encoding helpers
    // -----------------------------------------------------------------------

    static void sendNewOrderSingle(
            final Publication publication,
            final String clOrdId,
            final String symbol,
            final Side side,
            final OrdType ordType,
            final double orderQty,
            final double price) {

        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final NewOrderSingleEncoder encoder      = new NewOrderSingleEncoder();
        final ExpandableArrayBuffer buffer       = new ExpandableArrayBuffer(256);

        encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
                .side(side)
                .ordType(ordType)
                .orderQty(orderQty)
                .price(price)
                .clOrdId(clOrdId)
                .symbol(symbol);

        long result;
        do {
            result = publication.offer(buffer, 0, encoder.limit());
        } while (result < 0 && result != Publication.CLOSED);
    }

    // -----------------------------------------------------------------------
    // Decoding helpers
    // -----------------------------------------------------------------------

    static void onExecutionReport(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        final MessageHeaderDecoder    headerDecoder = new MessageHeaderDecoder();
        final ExecutionReportDecoder  decoder       = new ExecutionReportDecoder();

        headerDecoder.wrap(buffer, offset);
        decoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());

        System.out.printf(
                "[Client] Received ExecutionReport → orderId=%s clOrdId=%s execId=%s symbol=%s " +
                "execType=%s ordStatus=%s side=%s leavesQty=%.2f cumQty=%.2f avgPx=%.2f%n",
                decoder.orderId(),
                decoder.clOrdId(),
                decoder.execId(),
                decoder.symbol(),
                decoder.execType(),
                decoder.ordStatus(),
                decoder.side(),
                decoder.leavesQty(),
                decoder.cumQty(),
                decoder.avgPx());
    }
}
