package com.example.pulsar.producer;

import com.example.pulsar.constant.Serialization;
import org.apache.pulsar.shade.org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ProducerFactory implements PulsarProducerFactory {

    Map<String, ImmutableTriple<Class<?>, Serialization, Optional<String>>> producers = new ConcurrentHashMap<>();

    @Override
    public boolean createProducer(String topic) {
        return createProducer(topic, byte[].class, Serialization.BYTE);
    }

    @Override
    public boolean createProducer(String topic, Class<?> clazz) {
        producers.put(topic, new ImmutableTriple<>(clazz, Serialization.JSON, Optional.empty()));
        return true;
    }

    @Override
    public boolean createProducer(String topic, Class<?> clazz, Serialization serialization) {
        producers.put(topic, new ImmutableTriple<>(clazz, serialization, Optional.empty()));
        return true;
    }

    @Override
    public boolean createProducer(String topic, String namespace, Class<?> clazz, Serialization serialization) {
        producers.put(topic, new ImmutableTriple<>(clazz, serialization, Optional.of(namespace)));
        return true;
    }

    @Override
    public boolean createProducer(String topic, String namespace, Class<?> clazz) {
        producers.put(topic, new ImmutableTriple<>(clazz, Serialization.JSON, Optional.of(namespace)));
        return true;
    }

    @Override
    public Map<String, ImmutableTriple<Class<?>, Serialization, Optional<String>>> getProducers() {
        return producers;
    }
}
