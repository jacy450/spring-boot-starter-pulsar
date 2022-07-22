package com.example.pulsar.properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PulsarProperties {
    private String serviceUrl;
    private String tenant;
    private String namespace;
    private Integer operationTimeout;
    private int ioThreads;
    private int listenerThreads;
}
