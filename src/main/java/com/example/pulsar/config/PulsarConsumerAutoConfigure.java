package com.example.pulsar.config;

import com.example.pulsar.client.PulsarClientHolder;
import com.example.pulsar.exception.PulsarAutoConfigException;
import com.example.pulsar.listener.BaseMessageListener;
import com.example.pulsar.listener.PulsarListener;
import com.example.pulsar.listener.ThreadPool;
import com.example.pulsar.properties.MultiPulsarProperties;
import com.example.pulsar.utils.TopicUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.shade.org.apache.commons.lang.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarConsumerAutoConfigure implements ApplicationRunner {
    private final PulsarClientHolder holder;
    private final MultiPulsarProperties properties;
    @Autowired(required = false)
    private List<BaseMessageListener> listeners;

    public PulsarConsumerAutoConfigure(PulsarClientHolder holder, MultiPulsarProperties properties) {
        this.holder = holder;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!CollectionUtils.isEmpty(this.listeners)) {
            try {
                for (BaseMessageListener listener : this.listeners) {
                    PulsarListener pulsarListener = AnnotationUtils.findAnnotation(listener.getClass(), PulsarListener.class);
                    if (Objects.nonNull(pulsarListener)) {
                        String sourceName = pulsarListener.sourceName();
                        PulsarClient client = this.holder.getOrDefault(sourceName, null);
                        if (Objects.isNull(client)) {
                            log.error("[Pulsar] 数据源对应PulsarClient不存在，sourceName is {}", sourceName);
                            continue;
                        }
                        ConsumerBuilder<String> consumerBuilder = client.newConsumer(Schema.STRING)
                                .receiverQueueSize(pulsarListener.receiverQueueSize());
                        if (pulsarListener.topics().length > 0) {
                            if (Boolean.TRUE.equals(listener.enableAsync())) {
                                log.info("[Pulsar] 消费者开启异步消费，开始初始化消费线程池....");
                                ThreadPool threadPool = pulsarListener.threadPool();
                                listener.initThreadPool(threadPool.coreThreads(),
                                        threadPool.maxCoreThreads(),
                                        threadPool.keepAliveTime(),
                                        threadPool.maxQueueLength(),
                                        threadPool.threadPoolName());
                            }
                            ArrayList<String> topics = new ArrayList<>(pulsarListener.topics().length);
                            String tenant = StringUtils.hasText(pulsarListener.tenant()) ? pulsarListener.tenant() : this.properties.getSources().get(sourceName).getTenant();
                            String namespace = StringUtils.hasText(pulsarListener.namespace()) ? pulsarListener.namespace() : this.properties.getSources().get(sourceName).getNamespace();
                            if (!StringUtils.hasText(tenant) || !StringUtils.hasText(namespace)) {
                                log.error("[Pulsar] 消费者初始化失败，subscriptionName is {},sourceName is {},tenant is {},namespace is {}", pulsarListener.subscriptionName(), sourceName, tenant, namespace);
                                continue;
                            }

                            Boolean persistent = pulsarListener.persistent();
                            /**
                             * 处理topics
                             */
                            for (String topic : pulsarListener.topics()) {
                                topics.add(TopicUtil.generateTopic(persistent, tenant, namespace, topic));
                            }
                            consumerBuilder.topics(topics);
                            /**
                             * 处理订阅名称
                             */
                            String subscriptionName = StringUtils.hasText(pulsarListener.subscriptionName()) ? pulsarListener.subscriptionName() :
                                    "subscription_" + RandomStringUtils.random(3);
                            consumerBuilder.subscriptionName(subscriptionName);
                            consumerBuilder.ackTimeout(Long.parseLong(pulsarListener.ackTimeout()), TimeUnit.MILLISECONDS);
                            consumerBuilder.subscriptionType(pulsarListener.subscriptionType());
                            consumerBuilder.messageListener(listener);
                            Consumer<String> consumer = consumerBuilder.subscribe();
                            log.info("[Pulsar] Consumer初始化完毕, sourceName is {}, topic is {},", sourceName, consumer.getTopic());
                        }
                    }
                }
            } catch (PulsarClientException e) {
                throw new PulsarAutoConfigException("[Pulsar] consumer初始化异常", e);
            }
        } else {
            log.warn("[Pulsar] 未发现有Consumer");
        }
    }
}
