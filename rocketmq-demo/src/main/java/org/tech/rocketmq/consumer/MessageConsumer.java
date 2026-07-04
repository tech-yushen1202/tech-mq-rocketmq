package org.tech.rocketmq.consumer;

import org.tech.rocketmq.entity.MessageRecord;
import org.tech.rocketmq.repository.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 普通消息消费者
 * 
 * <p>监听 demo-topic 主题的所有消息（selectorExpression = "*"），
 * 将消费的消息持久化到数据库。
 * 
 * <p>使用方式：
 * <ul>
 *   <li>监听 Topic：demo-topic</li>
 *   <li>消费者组：demo-consumer-group</li>
 *   <li>消费模式：并发消费（默认）</li>
 * </ul>
 */
@RocketMQMessageListener(
        topic = "demo-topic",
        consumerGroup = "demo-consumer-group",
        selectorExpression = "*"
)
@Component
public class MessageConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 消息消费处理方法
     * 
     * <p>当收到消息时自动调用此方法。解析 JSON 格式的消息内容，
     * 提取 messageId、content、messageType，然后持久化到数据库。
     * 
     * @param message 消息内容（JSON 格式字符串）
     * @throws RuntimeException 消息处理失败时抛出异常，触发消息重试
     */
    @Override
    public void onMessage(String message) {
        logger.info("收到消息: {}", message);
        
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String messageId = jsonNode.has("messageId") ? jsonNode.get("messageId").asText() : null;
            String content = jsonNode.has("content") ? jsonNode.get("content").asText() : null;
            String messageType = jsonNode.has("messageType") ? jsonNode.get("messageType").asText() : "NORMAL";

            MessageRecord record = MessageRecord.builder()
                    .messageId(messageId)
                    .topic("demo-topic")
                    .messageType(messageType)
                    .content(content)
                    .status("CONSUMED")
                    .build();
            
            messageRecordRepository.save(record);
            
            logger.info("消息处理成功并持久化 - messageId: {}", messageId);
        } catch (Exception e) {
            logger.error("消息处理失败: {}", message, e);
            throw new RuntimeException("消息处理失败", e);
        }
    }
}