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
 * Tag 过滤消息消费者
 * 
 * <p>监听 tag-topic 主题，但只消费 Tag 为 "important" 的消息。
 * 
 * <p>Tag 过滤机制：
 * <ul>
 *   <li>Tag 是 Topic 下的二级分类，用于消息过滤</li>
 *   <li>通过 selectorExpression 指定需要消费的 Tag</li>
 *   <li>支持多个 Tag 用 || 连接，如 "tag1||tag2"</li>
 *   <li>只有匹配的消息才会投递到该消费者</li>
 * </ul>
 * 
 * <p>配置说明：
 * <ul>
 *   <li>监听 Topic：tag-topic</li>
 *   <li>消费者组：tag-consumer-group</li>
 *   <li>过滤表达式：selectorExpression = "important"（只消费 important 标签的消息）</li>
 * </ul>
 */
@RocketMQMessageListener(
        topic = "tag-topic",
        consumerGroup = "tag-consumer-group",
        selectorExpression = "important"
)
@Component
public class TagFilterConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(TagFilterConsumer.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Tag 过滤消息消费处理方法
     * 
     * <p>只处理 Tag 为 "important" 的消息，其他 Tag 的消息会被过滤掉。
     * 将消费的消息持久化到数据库。
     * 
     * @param message 消息内容（JSON 格式字符串）
     * @throws RuntimeException 消息处理失败时抛出异常，触发消息重试
     */
    @Override
    public void onMessage(String message) {
        logger.info("收到Tag过滤消息(仅important): {}", message);

        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String messageId = jsonNode.has("messageId") ? jsonNode.get("messageId").asText() : null;
            String content = jsonNode.has("content") ? jsonNode.get("content").asText() : null;

            MessageRecord record = MessageRecord.builder()
                    .messageId(messageId)
                    .topic("tag-topic")
                    .messageType("TAG_FILTER")
                    .content(content)
                    .status("CONSUMED")
                    .build();

            messageRecordRepository.save(record);

            logger.info("Tag过滤消息处理成功 - messageId: {}", messageId);
        } catch (Exception e) {
            logger.error("Tag过滤消息处理失败: {}", message, e);
            throw new RuntimeException("Tag过滤消息处理失败", e);
        }
    }
}