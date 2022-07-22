package com.example.pulsar.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "pulsar")
public class PulsarProperties {
    private String serviceUrl;
    private String tenant;
    private String namespace;
    private Integer operationTimeout;
    private int ioThreads;
    private int listenerThreads;
}
