package com.example.pulsar.config;

import com.example.pulsar.exception.PulsarAutoConfigException;
import com.example.pulsar.properties.PulsarProperties;
import com.example.pulsar.template.PulsarTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
@ConditionalOnExpression("!'${pulsar.serviceUrl}'.isEmpty()")
@EnableConfigurationProperties(PulsarProperties.class)
public class PulsarAutoConfiguration {

    @Bean
    public PulsarClient pulsarClient(PulsarProperties properties) {
        try {
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(properties.getServiceUrl())
                    .operationTimeout(properties.getOperationTimeout(), TimeUnit.SECONDS)
                    .listenerThreads(properties.getListenerThreads())
                    .ioThreads(properties.getIoThreads())
                    .build();
            log.info("[Pulsar] Client实例化成功,  serviceUrl is {}", properties.getServiceUrl());
            return client;
        } catch (PulsarClientException e) {
            log.error("[Pulsar] Client实例化失败！");
            throw new PulsarAutoConfigException("[Pulsar] Client实例化失败！", e);
        }
    }

    @Bean
    public PulsarTemplate pulsarTemplate(PulsarClient pulsarClient, PulsarProperties pulsarProperties) {
        return new PulsarTemplate(pulsarClient, pulsarProperties);
    }
}
