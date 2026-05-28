package io.aeron.koans.cluster;

import io.aeron.koans.sbe.ResponseStatus;

record Result(ResponseStatus status, long value, String error) {}
