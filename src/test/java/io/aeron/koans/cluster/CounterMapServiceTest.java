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
    void decrement_canGoNegative() {
        service.apply(CommandType.INCREMENT, 1, "hits");
        Result r = service.apply(CommandType.DECREMENT, 5, "hits");
        assertEquals(ResponseStatus.OK, r.status());
        assertEquals(-4L, r.value());
    }

    @Test
    void decrement_wouldGoBelowLongMinValue_returnsUnderflow() {
        // hits = 1; dec Long.MAX_VALUE → hits = Long.MIN_VALUE + 2; dec 3 → underflows
        service.apply(CommandType.INCREMENT, 1, "hits");
        service.apply(CommandType.DECREMENT, Long.MAX_VALUE, "hits");
        Result r = service.apply(CommandType.DECREMENT, 3L, "hits");
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
