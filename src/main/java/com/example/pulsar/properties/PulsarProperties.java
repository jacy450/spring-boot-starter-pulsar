package com.example.pulsar.properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PulsarProperties {
    /**
     * pulsar服务地址
     */
    private String serviceUrl;
    /**
     * 租户
     */
    private String tenant;
    /**
     * 命名空间
     */
    private String namespace;
    /**
     * 是否开启TCP不延迟
     */
    private Boolean enableTcpNoDelay;
    /**
     * 操作超时，单位毫秒
     */
    private Integer operationTimeout;
    /**
     * 消费者监听线程数
     */
    private Integer listenerThreads;
    /**
     * IO线程数
     */
    private Integer ioThreads;

    /**
     * 是否开启TCP不延迟
     */
    private Boolean defaultEnableTcpNoDelay = true;
    /**
     * 操作超时，单位秒
     */
    private Integer defaultOperationTimeout = 30;
    /**
     * 消费者监听线程数
     */
    private Integer defaultListenerThreads = 1;
    /**
     * IO线程数
     */
    private Integer defaultIoThreads = 1;
}
