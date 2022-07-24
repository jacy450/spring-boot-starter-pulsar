package com.example.pulsar;

import com.example.pulsar.template.PulsarTemplate;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@SpringBootTest(classes = {com.example.pulsar.Application.class})
class SpringBootStarterPulsarApplicationTests {

    @Autowired
    private PulsarTemplate pulsarTemplate;

    CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void test() throws PulsarClientException, ExecutionException, InterruptedException {
        pulsarTemplate.createBuilder()
                .sourceName("second")
                .persistent(true)
                .tenant("public")
                .namespace("default")
                .topic("my-topic")
                .send("my-topic");
        Thread.sleep(1000);
    }
}
