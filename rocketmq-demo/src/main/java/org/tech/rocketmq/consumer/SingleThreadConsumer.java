package org.tech.rocketmq.consumer;

import org.tech.rocketmq.entity.MessageRecord;
import org.tech.rocketmq.repository.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 单线程消息消费者
 * 
 * <p>演示 RocketMQ 的单线程消费功能，所有消息按顺序依次处理。
 * 通过设置 consumeThreadNumber = 1 实现单线程消费。
 * 
 * <p>单线程消费特点：
 * <ul>
 *   <li>所有消息在同一个线程中依次处理</li>
 *   <li>保证消息处理的顺序性</li>
 *   <li>吞吐量较低，适用于对顺序有要求的场景</li>
 * </ul>
 * 
 * <p>配置说明：
 * <ul>
 *   <li>监听 Topic：single-thread-topic</li>
 *   <li>消费者组：single-thread-consumer-group</li>
 *   <li>消费模式：CONCURRENTLY（并发模式）</li>
 *   <li>线程数：1（通过 consumeThreadNumber 限制）</li>
 * </ul>
 * 
 * <p>注意：与 ORDERLY 模式的区别：
 * <ul>
 *   <li>ORDERLY - 每个队列一个线程，保证队列内顺序</li>
 *   <li>单线程 - 所有队列共享一个线程，保证全局顺序</li>
 * </ul>
 */
@RocketMQMessageListener(
        topic = "single-thread-topic",
        consumerGroup = "single-thread-consumer-group",
        selectorExpression = "*",
        consumeMode = ConsumeMode.CONCURRENTLY,
        consumeThreadNumber = 1
)
@Component
public class SingleThreadConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(SingleThreadConsumer.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 单线程消息消费处理方法
     * 
     * <p>所有消息在同一个线程中处理，保证顺序性。
     * 通过日志可以观察到线程名称始终相同。
     * 
     * @param message 消息内容（JSON 格式字符串）
     * @throws RuntimeException 消息处理失败时抛出异常，触发消息重试
     */
    @Override
    public void onMessage(String message) {
        String threadName = Thread.currentThread().getName();
        logger.info("单线程消费消息 - thread: {}, message: {}", threadName, message);

        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String messageId = jsonNode.has("messageId") ? jsonNode.get("messageId").asText() : null;
            String content = jsonNode.has("content") ? jsonNode.get("content").asText() : null;

            MessageRecord record = MessageRecord.builder()
                    .messageId(messageId)
                    .topic("single-thread-topic")
                    .messageType("SINGLE_THREAD")
                    .content(content)
                    .status("CONSUMED")
                    .build();

            messageRecordRepository.save(record);

            logger.info("单线程消费完成 - thread: {}, messageId: {}", threadName, messageId);

        } catch (Exception e) {
            logger.error("单线程消费失败 - thread: {}, message: {}", threadName, message, e);
            throw new RuntimeException("单线程消费失败", e);
        }
    }
}