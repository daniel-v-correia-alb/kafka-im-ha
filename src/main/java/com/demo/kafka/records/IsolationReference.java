package com.demo.kafka.records;

import lombok.Getter;

import java.time.Instant;

@Getter
public class IsolationReference {
    private Instant tsRead;
    private Instant tsWrite;
    private Instant tsEventReference;

    public IsolationReference() {
    }
}
