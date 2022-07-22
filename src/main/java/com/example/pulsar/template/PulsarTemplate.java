package com.example.pulsar.template;

import com.example.pulsar.properties.PulsarProperties;
import com.example.pulsar.utils.TopicUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.util.StringUtils;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class PulsarTemplate {

    private final PulsarProperties pulsarProperties;

    private final PulsarClient pulsarClient;

    public PulsarTemplate(PulsarClient pulsarClient, PulsarProperties pulsarProperties) {
        this.pulsarClient = pulsarClient;
        this.pulsarProperties = pulsarProperties;
    }

    public Builder createBuilder() {
        return new Builder();
    }

    public class Builder {
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
            try {
                Producer<String> producer = PulsarTemplate.this.pulsarClient
                        .newProducer(Schema.STRING)
                        .topic(finalTopic)
                        .create();
                log.info("[Pulsar] Producer实例化成功, topic is {}",finalTopic);
                return producer.sendAsync(msg);
            } catch (PulsarClientException e) {
                log.error("[Pulsar] Producer实例化失败，topic is {}",finalTopic);
                throw e;
            }
        }

        private String generateTopic() {
            if (!StringUtils.hasText(this.topic)) {
                log.error("[Pulsar] Topic 为空，无法发送消息, topic is {}", this.topic);
            }
            String finalTenant = StringUtils.hasText(this.tenant) ?
                    this.tenant : PulsarTemplate.this.pulsarProperties.getTenant();
            String finalNamespace = StringUtils.hasText(this.namespace) ?
                    this.namespace : PulsarTemplate.this.pulsarProperties.getNamespace();
            if (!StringUtils.hasText(finalTenant) || !StringUtils.hasText(finalNamespace)) {
                log.error("[Pulsar] 租户||命名空间为空，无法创建发送消息, tenant is {}, namespace is {}",
                        finalTenant, finalNamespace);
            }
            Boolean finalPersistent = Objects.nonNull(this.persistent) ?
                    this.persistent : Boolean.TRUE;
            return TopicUtil.generateTopic(finalPersistent, finalTenant, finalNamespace, this.topic);
        }
    }
}
