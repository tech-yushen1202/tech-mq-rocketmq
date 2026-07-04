package org.tech.rocketmq.consumer;

import org.tech.rocketmq.entity.MessageRecord;
import org.tech.rocketmq.repository.MessageRecordRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 手动确认消息消费者
 * 
 * <p>演示 RocketMQ 的手动确认消息消费结果功能。
 * 
 * <p>为什么使用 CommandLineRunner 而不是 @RocketMQMessageListener 注解：
 * <ul>
 *   <li>rocketmq-spring-boot-starter 提供的 {@link org.apache.rocketmq.spring.core.RocketMQListener}
 *       接口的 {@code onMessage(T message)} 方法返回 void，消息消费后自动确认</li>
 *   <li>若要手动控制确认结果（返回 CONSUME_SUCCESS 或 RECONSUME_LATER），
 *       必须实现原生的 {@link MessageListenerConcurrently} 接口</li>
 *   <li>rocketmq-spring-boot-starter 不支持通过注解方式使用 MessageListenerConcurrently，
 *       因此需要通过 CommandLineRunner 在应用启动时手动创建消费者</li>
 * </ul>
 * 
 * <p>手动确认机制：
 * <ul>
 *   <li>CONSUME_SUCCESS - 消费成功，消息从队列中移除</li>
 *   <li>RECONSUME_LATER - 消费失败，消息延迟重新投递</li>
 * </ul>
 * 
 * <p>配置说明：
 * <ul>
 *   <li>监听 Topic：manual-ack-topic</li>
 *   <li>消费者组：manual-ack-consumer-group</li>
 *   <li>消费模式：并发消费（CONCURRENTLY）</li>
 * </ul>
 * 
 * <p>重试策略：
 * <ul>
 *   <li>消费失败时返回 RECONSUME_LATER，消息会延迟重试</li>
 *   <li>重试次数达到 3 次后，记录为死信消息，返回 CONSUME_SUCCESS</li>
 * </ul>
 */
@Component
public class ManualAckConsumer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ManualAckConsumer.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private org.tech.rocketmq.config.RocketMQConfig rocketMQConfig;

    /**
     * 应用启动时初始化消费者
     * 
     * <p>使用 RocketMQ 原生 API 创建消费者，注册消息监听器。
     * 监听器中实现手动确认逻辑。
     * 
     * @param args 命令行参数
     * @throws Exception 初始化异常
     */
    @Override
    public void run(String... args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("manual-ack-consumer-group");
        consumer.setNamesrvAddr(rocketMQConfig.getNameServerAddr());
        consumer.subscribe("manual-ack-topic", "*");
        
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String message = new String(msg.getBody());
                logger.info("收到手动确认消息 - messageId: {}, reconsumeTimes: {}", 
                        msg.getMsgId(), msg.getReconsumeTimes());

                try {
                    JsonNode jsonNode = objectMapper.readTree(message);
                    String messageId = jsonNode.has("messageId") ? jsonNode.get("messageId").asText() : msg.getMsgId();
                    String content = jsonNode.has("content") ? jsonNode.get("content").asText() : null;

                    MessageRecord record = MessageRecord.builder()
                            .messageId(messageId)
                            .topic("manual-ack-topic")
                            .messageType("MANUAL_ACK")
                            .content(content)
                            .status("CONSUMED")
                            .build();

                    messageRecordRepository.save(record);

                    logger.info("手动确认消息处理成功，返回CONSUME_SUCCESS - messageId: {}", messageId);

                } catch (Exception e) {
                    logger.error("手动确认消息处理失败，返回RECONSUME_LATER - messageId: {}, error: {}", 
                            msg.getMsgId(), e.getMessage());
                    
                    if (msg.getReconsumeTimes() >= 3) {
                        logger.warn("消息重试次数已达上限，记录死信 - messageId: {}", msg.getMsgId());
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        
        consumer.start();
        logger.info("手动确认消费者启动成功");
    }
}