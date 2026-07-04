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

/**
 * 并发消费消息消费者
 * 
 * <p>演示 RocketMQ 的并发消费功能，使用多线程同时消费消息，提高吞吐量。
 * 
 * <p>为什么使用 CommandLineRunner 而不是 @RocketMQMessageListener 注解：
 * <ul>
 *   <li>虽然 @RocketMQMessageListener 注解支持通过 {@code consumeThreadNumber} 设置线程数，
 *       但使用原生 API 可以更灵活地控制消费者配置（如最小/最大线程数、批量消费等）</li>
 *   <li>为了演示如何使用 RocketMQ 原生 API 创建消费者，展示更底层的配置方式</li>
 *   <li>同时也为了与 ManualAckConsumer 保持一致的实现模式</li>
 * </ul>
 * 
 * <p>并发消费特点：
 * <ul>
 *   <li>多个线程同时消费不同队列的消息</li>
 *   <li>同一队列的消息可能被不同线程消费（不保证顺序）</li>
 *   <li>适用于对消息顺序无要求、追求高吞吐量的场景</li>
 * </ul>
 * 
 * <p>配置说明：
 * <ul>
 *   <li>监听 Topic：concurrent-topic</li>
 *   <li>消费者组：concurrent-consumer-group</li>
 *   <li>消费模式：并发消费（CONCURRENTLY）</li>
 *   <li>最小线程数：10</li>
 *   <li>最大线程数：20</li>
 * </ul>
 */
@Component
public class ConcurrentConsumer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentConsumer.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private org.tech.rocketmq.config.RocketMQConfig rocketMQConfig;

    /**
     * 应用启动时初始化消费者
     * 
     * <p>使用 RocketMQ 原生 API 创建消费者，设置线程池参数。
     * 通过日志可以观察到不同线程处理不同消息。
     * 
     * @param args 命令行参数
     * @throws Exception 初始化异常
     */
    @Override
    public void run(String... args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("concurrent-consumer-group");
        consumer.setNamesrvAddr(rocketMQConfig.getNameServerAddr());
        consumer.subscribe("concurrent-topic", "*");
        
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(20);
        
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String message = new String(msg.getBody());
                String threadName = Thread.currentThread().getName();
                
                logger.info("并发消费消息 - thread: {}, messageId: {}", threadName, msg.getMsgId());

                try {
                    JsonNode jsonNode = objectMapper.readTree(message);
                    String messageId = jsonNode.has("messageId") ? jsonNode.get("messageId").asText() : msg.getMsgId();
                    String content = jsonNode.has("content") ? jsonNode.get("content").asText() : null;

                    MessageRecord record = MessageRecord.builder()
                            .messageId(messageId)
                            .topic("concurrent-topic")
                            .messageType("CONCURRENT")
                            .content(content)
                            .status("CONSUMED")
                            .build();

                    messageRecordRepository.save(record);

                    logger.info("并发消费完成 - thread: {}, messageId: {}", threadName, messageId);

                } catch (Exception e) {
                    logger.error("并发消费失败 - thread: {}, messageId: {}", threadName, msg.getMsgId(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        
        consumer.start();
        logger.info("并发消费者启动成功");
    }
}