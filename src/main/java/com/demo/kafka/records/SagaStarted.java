package com.demo.kafka.records;

import lombok.Getter;

import java.time.Instant;

@Getter
public class SagaStarted {
    private final String sagaId;
    private final Instant tsEventReference;
    private final int sagaTimeout;

    public SagaStarted(String sagaId, Instant tsEventReference, int sagaTimeout) {
        this.sagaId = sagaId;
        this.tsEventReference = tsEventReference;
        this.sagaTimeout = sagaTimeout;
    }
}
