package com.example.pulsar.exception;

public class PulsarAutoConfigException extends RuntimeException {

    public PulsarAutoConfigException(String msg) {
        super(msg);
    }

    public PulsarAutoConfigException(String msg, Throwable e) {
        super(msg, e);
    }
}
