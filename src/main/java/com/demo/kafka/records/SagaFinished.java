package com.demo.kafka.records;

import lombok.Getter;

@Getter
public class SagaFinished {
    private final String sagaId;

    public SagaFinished(String sagaId) {
        this.sagaId = sagaId;
    }
}
