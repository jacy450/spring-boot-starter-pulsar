package com.example.pulsar.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Setter
@Getter
@ConfigurationProperties(prefix = "pulsar")
public class MultiPulsarProperties {

    public static final String DEFAULT_SOURCE_NAME = "default";
    private Map<String, PulsarProperties> sources;
}
