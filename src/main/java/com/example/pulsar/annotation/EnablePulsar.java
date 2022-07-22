package com.example.pulsar.annotation;

import com.example.pulsar.config.PulsarAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@Component
@Import({PulsarAutoConfiguration.class})
public @interface EnablePulsar {

}
