package com.example.pulsar.producer;

import com.example.pulsar.constant.Serialization;
import org.apache.pulsar.shade.org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.Map;
import java.util.Optional;

public interface PulsarProducerFactory {
    boolean createProducer(String topic);

    boolean createProducer(String topic, Class<?> clazz);

    boolean createProducer(String topic, Class<?> clazz, Serialization serialization);

    boolean createProducer(String topic, String namespace, Class<?> clazz, Serialization serialization);

    boolean createProducer(String topic, String namespace, Class<?> clazz);

    Map<String, ImmutableTriple<Class<?>, Serialization, Optional<String>>> getProducers();
}
