package com.example.pulsar.exception;

public class PulsarBusinessException extends RuntimeException {

    public PulsarBusinessException(String msg) {
        super(msg);
    }

    public PulsarBusinessException(String msg, Throwable e) {
        super(msg, e);
    }
}
