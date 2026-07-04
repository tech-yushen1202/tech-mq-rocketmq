package org.tech.rocketmq.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ 配置类
 * 
 * <p>用于从 application.yml 中读取 RocketMQ 相关配置参数。
 * 通过 @ConfigurationProperties 注解实现配置绑定，前缀为 "rocketmq"。
 * 
 * <p>配置项说明：
 * <ul>
 *   <li>nameServerAddr - NameServer 地址，格式为 host:port，多个用分号分隔</li>
 *   <li>producerGroup - 普通生产者组名称</li>
 *   <li>consumerGroup - 普通消费者组名称</li>
 *   <li>transactionProducerGroup - 事务消息生产者组名称</li>
 * </ul>
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "rocketmq")
public class RocketMQConfig {

    /**
     * NameServer 地址，默认 localhost:9876
     */
    private String nameServerAddr = "localhost:9876";

    /**
     * 普通消息生产者组
     */
    private String producerGroup = "demo-producer-group";

    /**
     * 普通消息消费者组
     */
    private String consumerGroup = "demo-consumer-group";

    /**
     * 事务消息生产者组
     */
    private String transactionProducerGroup = "demo-transaction-producer-group";

    public String getNameServerAddr() {
        return nameServerAddr;
    }

    public void setNameServerAddr(String nameServerAddr) {
        this.nameServerAddr = nameServerAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTransactionProducerGroup() {
        return transactionProducerGroup;
    }

    public void setTransactionProducerGroup(String transactionProducerGroup) {
        this.transactionProducerGroup = transactionProducerGroup;
    }
}