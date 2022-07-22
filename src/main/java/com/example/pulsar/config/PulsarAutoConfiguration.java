package com.example.pulsar.config;

import com.example.pulsar.client.PulsarClientHolders.PulsarClientHolder;
import com.example.pulsar.properties.MultiPulsarProperties;
import com.example.pulsar.template.PulsarTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableConfigurationProperties(MultiPulsarProperties.class)
public class PulsarAutoConfiguration {

    @Bean
    public PulsarClientHolder pulsarClientHolder(MultiPulsarProperties properties) {
        return new PulsarClientHolder(properties);
    }

    @Bean
    public PulsarTemplate pulsarTemplate(PulsarClientHolder holder, MultiPulsarProperties properties) {
        return new PulsarTemplate(holder, properties);
    }
}
