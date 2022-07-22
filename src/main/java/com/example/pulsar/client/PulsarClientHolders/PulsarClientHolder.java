package com.example.pulsar.client.PulsarClientHolders;

import com.example.pulsar.exception.PulsarAutoConfigException;
import com.example.pulsar.properties.MultiPulsarProperties;
import com.example.pulsar.properties.PulsarProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.DisposableBean;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarClientHolder extends HashMap<String, PulsarClient> implements DisposableBean {

    public PulsarClientHolder(MultiPulsarProperties multiPulsarProperties) {
        Map<String, PulsarProperties> sources = multiPulsarProperties.getSources();

        this.putAll(Optional.ofNullable(sources).map(map -> {
            Map<String, PulsarClient> clients = new ConcurrentHashMap<>();
            for (String sourceName : map.keySet()) {
                try {
                    PulsarClient client = PulsarClient.builder()
                            .serviceUrl(map.get(sourceName).getServiceUrl())
                            .operationTimeout(map.get(sourceName).getOperationTimeout(), TimeUnit.SECONDS)
                            .listenerThreads(map.get(sourceName).getListenerThreads())
                            .ioThreads(map.get(sourceName).getIoThreads())
                            .build();
                    log.info("[Pulsar] Client实例化成功, sourceName is {}, serviceUrl is {}", sourceName, map.get(sourceName).getServiceUrl());
                    clients.put(sourceName, client);
                } catch (PulsarClientException e) {
                    log.error("[Pulsar] Client实例化失败！");
                    throw new PulsarAutoConfigException("[Pulsar] Client实例化失败！", e);
                }
            }
            return clients;
        }).get());
    }

    @Override
    public void destroy() {
        this.values().forEach(client -> {
            try {
                client.close();
                log.info("[pulsar] Client Close Success");
            } catch (PulsarClientException e) {
                log.error("[pulsar] Client Close Failed");
            }
        });
    }
}
