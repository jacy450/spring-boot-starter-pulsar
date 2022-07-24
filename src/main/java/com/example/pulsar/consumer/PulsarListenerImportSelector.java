package com.example.pulsar.consumer;

import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.type.AnnotationMetadata;

public class PulsarListenerImportSelector implements DeferredImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{PulsarBootstrapConfiguration.class.getName()};
    }
}
