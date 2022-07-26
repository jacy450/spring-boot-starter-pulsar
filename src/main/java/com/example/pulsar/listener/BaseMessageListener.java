package com.example.pulsar.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class BaseMessageListener implements MessageListener<String> {

    private Executor executor;

    /***
     * 初始化Consumer线程池
     * @param corePoolSize 核心线程数
     * @param maximumPoolSize 最大核心线程数
     * @param keepAliveTime 线程保活时长，单位分钟
     * @param maxQueueLength 最大等待队列长度
     * @return {@link Void }
     **/
    public void initThreadPool(Integer corePoolSize, Integer maximumPoolSize, Integer keepAliveTime,
                               Integer maxQueueLength, String threadPoolName) {
        new ThreadPoolExecutor(corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(maximumPoolSize),
                new ThreadFactoryBuilder().setNameFormat(threadPoolName + "%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        log.info("[Pulsar] Consumer消费线程池初始化成功！");
    }

    @Override
    public void received(Consumer<String> consumer, Message<String> msg) {
        if (Objects.nonNull(this.executor) && Boolean.TRUE.equals(this.enableAsync())) {
            this.executor.execute(() -> this.doReceived(consumer, msg));
        } else {
            this.doReceived(consumer, msg);
        }
    }

    /**
     * 消费消息
     * 自定义监听器实现方法
     * 消息如何响应由开发者决定：
     * Consumer#acknowledge
     * Consumer#reconsumeLater
     * Consumer#negativeAcknowledge
     *
     * @param consumer 消费者
     * @param msg      消息
     */
    protected abstract void doReceived(Consumer<String> consumer, Message<String> msg);

    /***
     * 是否开启异步消费，默认开启
     * @return {@link Boolean }
     **/
    public Boolean enableAsync() {
        return Boolean.TRUE;
    }
}
