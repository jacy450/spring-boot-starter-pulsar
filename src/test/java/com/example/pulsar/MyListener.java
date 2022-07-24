package com.example.pulsar;

import com.example.pulsar.listener.BaseMessageListener;
import com.example.pulsar.listener.PulsarListener;
import com.example.pulsar.listener.ThreadPool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

@PulsarListener(topics = {"my-topic"},
        threadPool = @ThreadPool(
                coreThreads = 2,
                maxCoreThreads = 3,
                threadPoolName = "test-thread-pool"))
public class MyListener extends BaseMessageListener {
    @Override
    protected void doReceived(Consumer<String> consumer, Message<String> msg) {
        System.out.println("成功消费消息：{ " + msg.getValue() + " }");
        try {
            consumer.acknowledge(msg);
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Boolean enableAsync() {
        return Boolean.TRUE;
    }
}
