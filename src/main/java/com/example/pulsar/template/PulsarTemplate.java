package com.example.pulsar.template;

import com.example.pulsar.client.PulsarClientHolders.PulsarClientHolder;
import com.example.pulsar.exception.PulsarBusinessException;
import com.example.pulsar.properties.MultiPulsarProperties;
import com.example.pulsar.utils.TopicUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class PulsarTemplate {

    /**
     * producer 缓存
     * key：topic，value：producer
     */
    private final ConcurrentHashMap<String, Producer<String>> producerCaches =
            new ConcurrentHashMap<>(64);

    private final MultiPulsarProperties properties;

    private final PulsarClientHolder holder;

    public PulsarTemplate(PulsarClientHolder holder, MultiPulsarProperties properties) {
        this.holder = holder;
        this.properties = properties;
    }

    public Builder createBuilder() {
        return new Builder();
    }

    public class Builder {
        /**
         * 数据源名称
         * 默认值：{@link MultiPulsarProperties#DEFAULT_SOURCE_NAME}
         */
        private String sourceName;
        /**
         * 是否持久化
         */
        private Boolean persistent;
        /**
         * 租户
         */
        private String tenant;
        /**
         * 命名空间
         */
        private String namespace;
        /**
         * 主题
         */
        private String topic;

        public Builder sourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public Builder persistent(Boolean persistent) {
            this.persistent = persistent;
            return this;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * 同步发送消息
         *
         * @param msg 消息
         * @return 消息ID
         */
        public MessageId send(String msg) throws ExecutionException, InterruptedException, PulsarClientException {
            try {
                MessageId messageId = this.sendAsync(msg).get();
                log.info("[Pulsar] Producer同步发送消息成功，msg is {}", msg);
                return messageId;
            } catch (InterruptedException | ExecutionException | PulsarClientException e) {
                log.error("[Pulsar] Producer同步发送消息失败，msg is {}", msg);
                throw e;
            }
        }

        /**
         * 异步发送消息
         *
         * @param msg 消息
         * @return CompletableFuture
         */
        public CompletableFuture<MessageId> sendAsync(String msg) throws PulsarClientException {
            String finalTopic = this.generateTopic();
            String sourceName = Optional.ofNullable(this.sourceName).orElse(MultiPulsarProperties.DEFAULT_SOURCE_NAME);

            try {
                Producer<String> producer = PulsarTemplate.this.producerCaches.getOrDefault(finalTopic, null);
                if (Objects.isNull(producer)) {
                    PulsarClient client = PulsarTemplate.this.holder.getOrDefault(sourceName, null);
                    if (Objects.isNull(client)) {
                        log.error("[Pulsar] 数据源对应PulsarClient不存在，sourceName is {}", sourceName);
                        throw new PulsarBusinessException("[Pulsar] 数据源对应PulsarClient不存在！");
                    }
                    producer = client.newProducer(Schema.STRING).topic(finalTopic).create();
                    PulsarTemplate.this.producerCaches.put(finalTopic, producer);
                    log.info("[Pulsar] Producer实例化成功，sourceName is {}, topic is {}", sourceName, finalTopic);
                }
                return producer.sendAsync(msg);
            } catch (PulsarClientException e) {
                log.error("[Pulsar] Producer实例化失败，topic is {}", finalTopic);
                throw e;
            }
        }

        /**
         * 拼接topic
         *
         * @return 完整topic路径
         */
        private String generateTopic() {
            if (!StringUtils.hasText(this.topic)) {
                log.error("[Pulsar] Topic 为空，无法发送消息, topic is {}", this.topic);
                throw new PulsarBusinessException("Topic不能为空");
            }
            String finalTenant = StringUtils.hasText(this.tenant) ?
                    this.tenant :
                    PulsarTemplate.this.properties.getSources().get(this.sourceName).getTenant();
            String finalNamespace = StringUtils.hasText(this.namespace) ?
                    this.namespace :
                    PulsarTemplate.this.properties.getSources().get(this.sourceName).getNamespace();
            if (!StringUtils.hasText(finalTenant) || !StringUtils.hasText(finalNamespace)) {
                log.error("[Pulsar] 租户||命名空间为空，无法创建发送消息, tenant is {}, namespace is {}",
                        finalTenant, finalNamespace);
                throw new PulsarBusinessException("租户||命名空间为空，无法发送消息");
            }
            Boolean finalPersistent = Objects.nonNull(this.persistent) ?
                    this.persistent : Boolean.TRUE;
            return TopicUtil.generateTopic(finalPersistent, finalTenant, finalNamespace, this.topic);
        }
    }
}
