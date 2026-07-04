package org.tech.rocketmq.service;

import org.tech.rocketmq.dto.MessageRequest;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQ 消息生产者服务
 * 提供多种消息发送模式：同步、异步、单向、延迟、顺序、批量
 */
@Service
public class ProducerService {

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 同步发送消息
     * 发送后等待 Broker 确认，返回发送结果
     * 适用于需要确保消息发送成功的场景，如重要业务数据传输
     *
     * @param request 消息请求对象，包含 topic、tags、content 等信息
     * @return 消息唯一标识 messageId
     * @throws RuntimeException 发送失败时抛出异常
     */
    public String sendSyncMessage(MessageRequest request) {
        String messageId = UUID.randomUUID().toString();
        String destination = request.getTopic() + ":" + (request.getTags() != null ? request.getTags() : "");
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(
                    destination,
                    buildMessage(request, messageId)
            );
            logger.info("同步消息发送成功 - messageId: {}, topic: {}, result: {}", 
                    messageId, request.getTopic(), sendResult.getSendStatus());
            return messageId;
        } catch (Exception e) {
            logger.error("同步消息发送失败 - messageId: {}, topic: {}", messageId, request.getTopic(), e);
            throw new RuntimeException("同步消息发送失败", e);
        }
    }

    /**
     * 异步发送消息
     * 发送后立即返回，通过回调函数获取发送结果
     * 适用于对响应时间要求较高，不需要立即知道发送结果的场景
     *
     * @param request 消息请求对象，包含 topic、tags、content 等信息
     * @return 消息唯一标识 messageId
     */
    public String sendAsyncMessage(MessageRequest request) {
        String messageId = UUID.randomUUID().toString();
        String destination = request.getTopic() + ":" + (request.getTags() != null ? request.getTags() : "");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        rocketMQTemplate.asyncSend(
                destination,
                buildMessage(request, messageId),
                new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        logger.info("异步消息发送成功 - messageId: {}, topic: {}", messageId, request.getTopic());
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable e) {
                        logger.error("异步消息发送失败 - messageId: {}, topic: {}", messageId, request.getTopic(), e);
                        latch.countDown();
                    }
                }
        );
        
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("异步消息等待超时 - messageId: {}", messageId, e);
        }
        
        return messageId;
    }

    /**
     * 单向发送消息
     * 发送即忘，不等待 Broker 确认，也不返回结果
     * 适用于对可靠性要求不高的场景，如日志收集、埋点上报
     *
     * @param request 消息请求对象，包含 topic、tags、content 等信息
     * @return 消息唯一标识 messageId
     */
    public String sendOnewayMessage(MessageRequest request) {
        String messageId = UUID.randomUUID().toString();
        String destination = request.getTopic() + ":" + (request.getTags() != null ? request.getTags() : "");
        
        rocketMQTemplate.sendOneWay(
                destination,
                buildMessage(request, messageId)
        );
        
        logger.info("单向消息发送完成 - messageId: {}, topic: {}", messageId, request.getTopic());
        return messageId;
    }

    /**
     * 延迟发送消息
     * 消息发送后不会立即投递，等待指定秒数后再投递
     * 适用于需要延迟处理的场景，如订单超时取消、定时任务触发
     *
     * @param request     消息请求对象，包含 topic、tags、content 等信息
     * @param delaySeconds 延迟秒数
     * @return 消息唯一标识 messageId
     * @throws RuntimeException 发送失败时抛出异常
     */
    public String sendDelayMessage(MessageRequest request, int delaySeconds) {
        String messageId = UUID.randomUUID().toString();
        String destination = request.getTopic() + ":" + (request.getTags() != null ? request.getTags() : "");
        
        try {
            SendResult sendResult = rocketMQTemplate.syncSendDelayTimeSeconds(
                    destination,
                    buildMessage(request, messageId),
                    delaySeconds
            );
            logger.info("延迟消息发送成功 - messageId: {}, topic: {}, delaySeconds: {}", 
                    messageId, request.getTopic(), delaySeconds);
            return messageId;
        } catch (Exception e) {
            logger.error("延迟消息发送失败 - messageId: {}, topic: {}", messageId, request.getTopic(), e);
            throw new RuntimeException("延迟消息发送失败", e);
        }
    }

    /**
     * 顺序发送消息
     * 相同 shardingKey 的消息会被发送到同一个队列，保证顺序消费
     * 适用于需要保证消息顺序的场景，如订单状态变更、流水日志
     *
     * @param request    消息请求对象，包含 topic、tags、content 等信息
     * @param shardingKey 分片键，相同值的消息保证顺序
     * @return 消息唯一标识 messageId
     * @throws RuntimeException 发送失败时抛出异常
     */
    public String sendOrderlyMessage(MessageRequest request, String shardingKey) {
        String messageId = UUID.randomUUID().toString();
        String destination = request.getTopic() + ":" + (request.getTags() != null ? request.getTags() : "");
        
        try {
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(
                    destination,
                    buildMessage(request, messageId),
                    shardingKey
            );
            logger.info("顺序消息发送成功 - messageId: {}, topic: {}, shardingKey: {}", 
                    messageId, request.getTopic(), shardingKey);
            return messageId;
        } catch (Exception e) {
            logger.error("顺序消息发送失败 - messageId: {}, topic: {}", messageId, request.getTopic(), e);
            throw new RuntimeException("顺序消息发送失败", e);
        }
    }

    /**
     * 批量发送消息
     * 将多条消息打包发送，减少网络开销，提高吞吐量
     * 适用于需要批量处理的场景，如批量数据同步
     *
     * @param topic    消息主题
     * @param contents 消息内容列表
     * @throws RuntimeException 发送失败时抛出异常
     */
    public void sendBatchMessage(String topic, List<String> contents) {
        String destination = topic;
        
        List<Message> messages = contents.stream()
                .map(content -> {
                    String messageId = UUID.randomUUID().toString();
                    String body = "{\"messageId\":\"" + messageId + "\",\"content\":\"" + content + "\"}";
                    return new Message(topic, body.getBytes(StandardCharsets.UTF_8));
                })
                .toList();
        
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(destination, messages, 30000);
            logger.info("批量消息发送成功 - topic: {}, count: {}", topic, messages.size());
        } catch (Exception e) {
            logger.error("批量消息发送失败 - topic: {}", topic, e);
            throw new RuntimeException("批量消息发送失败", e);
        }
    }

    /**
     * 构建 RocketMQ 消息对象
     * 将业务请求转换为 RocketMQ 标准消息格式
     *
     * @param request  消息请求对象
     * @param messageId 消息唯一标识
     * @return RocketMQ Message 对象
     */
    private org.apache.rocketmq.common.message.Message buildMessage(MessageRequest request, String messageId) {
        String body = String.format("{\"messageId\":\"%s\",\"content\":\"%s\",\"messageType\":\"%s\"}",
                messageId, request.getContent(), request.getMessageType());
        return new org.apache.rocketmq.common.message.Message(
                request.getTopic(),
                request.getTags(),
                messageId,
                body.getBytes(StandardCharsets.UTF_8)
        );
    }

    /**
     * 发送带 Tag 的消息
     * 
     * <p>Tag 是 RocketMQ 的消息过滤机制，消费者可以通过 selectorExpression 过滤只接收
     * 指定 Tag 的消息。适用于需要按业务类别分类处理消息的场景。
     *
     * @param topic   消息主题
     * @param tag     消息标签，用于消费者过滤
     * @param content 消息内容
     * @return 消息唯一标识 messageId
     * @throws RuntimeException 发送失败时抛出异常
     */
    public String sendMessageWithTag(String topic, String tag, String content) {
        String messageId = UUID.randomUUID().toString();
        String destination = topic + ":" + (tag != null ? tag : "");
        String body = String.format("{\"messageId\":\"%s\",\"content\":\"%s\",\"messageType\":\"TAGGED\"}",
                messageId, content);
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(
                    destination,
                    new org.apache.rocketmq.common.message.Message(
                            topic,
                            tag,
                            messageId,
                            body.getBytes(StandardCharsets.UTF_8)
                    )
            );
            logger.info("带Tag消息发送成功 - messageId: {}, topic: {}, tag: {}", messageId, topic, tag);
            return messageId;
        } catch (Exception e) {
            logger.error("带Tag消息发送失败 - messageId: {}, topic: {}", messageId, topic, e);
            throw new RuntimeException("带Tag消息发送失败", e);
        }
    }

    /**
     * 发送消息到指定 Topic（不带 Tag）
     * 
     * <p>简化的消息发送方法，直接指定 Topic 和内容，不使用 Tag 过滤。
     *
     * @param topic   消息主题
     * @param content 消息内容
     * @return 消息唯一标识 messageId
     * @throws RuntimeException 发送失败时抛出异常
     */
    public String sendMessageToTopic(String topic, String content) {
        String messageId = UUID.randomUUID().toString();
        String body = String.format("{\"messageId\":\"%s\",\"content\":\"%s\",\"messageType\":\"TEST\"}",
                messageId, content);
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(
                    topic,
                    new org.apache.rocketmq.common.message.Message(
                            topic,
                            null,
                            messageId,
                            body.getBytes(StandardCharsets.UTF_8)
                    )
            );
            logger.info("消息发送成功 - messageId: {}, topic: {}", messageId, topic);
            return messageId;
        } catch (Exception e) {
            logger.error("消息发送失败 - messageId: {}, topic: {}", messageId, topic, e);
            throw new RuntimeException("消息发送失败", e);
        }
    }

    /**
     * 批量发送消息到指定 Topic
     * 
     * <p>循环发送指定数量的消息，用于测试并发消费、消息堆积等场景。
     *
     * @param topic  消息主题
     * @param count  发送消息数量
     */
    public void sendMessagesToTopic(String topic, int count) {
        for (int i = 0; i < count; i++) {
            String messageId = UUID.randomUUID().toString();
            String body = String.format("{\"messageId\":\"%s\",\"content\":\"message_%d\",\"messageType\":\"TEST\"}",
                    messageId, i);
            try {
                rocketMQTemplate.syncSend(
                        topic,
                        new org.apache.rocketmq.common.message.Message(
                                topic,
                                null,
                                messageId,
                                body.getBytes(StandardCharsets.UTF_8)
                        )
                );
                logger.info("批量发送消息 {} - messageId: {}", i + 1, messageId);
            } catch (Exception e) {
                logger.error("批量发送消息失败 - index: {}, topic: {}", i, topic, e);
            }
        }
    }
}