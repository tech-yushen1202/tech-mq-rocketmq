package org.tech.rocketmq.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 消息请求 DTO（Data Transfer Object）
 * 
 * <p>用于接收客户端发送的消息请求参数，包含消息的基本信息。
 * 使用 Lombok 注解自动生成 getter/setter、构造函数和 Builder 模式。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageRequest {

    /**
     * 消息主题（Topic）
     * <p>RocketMQ 通过 Topic 对消息进行分类，生产者发送消息到指定 Topic，
     * 消费者订阅指定 Topic 的消息。
     */
    private String topic;

    /**
     * 消息标签（Tag）
     * <p>Tag 是 Topic 下的二级分类，用于消息过滤。
     * 消费者可以通过 Tag 过滤只接收感兴趣的消息，格式如 "tag1||tag2"。
     */
    private String tags;

    /**
     * 消息内容
     * <p>消息的实际业务数据，通常为 JSON 格式字符串。
     */
    private String content;

    /**
     * 消息类型
     * <p>自定义字段，用于标识消息的业务类型，如：SYNC、ASYNC、DELAY、ORDERLY、TRANSACTION 等。
     */
    private String messageType;
}