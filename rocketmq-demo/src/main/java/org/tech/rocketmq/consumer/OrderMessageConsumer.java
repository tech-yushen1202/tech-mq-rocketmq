package org.tech.rocketmq.consumer;

import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 顺序消息消费者
 * 
 * <p>监听 order-topic 主题的消息，使用顺序消费模式（ConsumeMode.ORDERLY）。
 * 
 * <p>顺序消费机制：
 * <ul>
 *   <li>相同 shardingKey 的消息会被发送到同一个 Message Queue</li>
 *   <li>同一个 Queue 只被一个消费者实例消费</li>
 *   <li>每个 Queue 使用单线程消费，保证消息顺序</li>
 * </ul>
 * 
 * <p>配置说明：
 * <ul>
 *   <li>监听 Topic：order-topic</li>
 *   <li>消费者组：order-consumer-group</li>
 *   <li>消费模式：ConsumeMode.ORDERLY（顺序消费）</li>
 * </ul>
 */
@RocketMQMessageListener(
        topic = "order-topic",
        consumerGroup = "order-consumer-group",
        selectorExpression = "*",
        consumeMode = ConsumeMode.ORDERLY
)
@Component
public class OrderMessageConsumer implements RocketMQListener<String> {

    private static final Logger logger = LoggerFactory.getLogger(OrderMessageConsumer.class);

    /**
     * 顺序消息消费处理方法
     * 
     * <p>按消息发送顺序消费，保证相同 shardingKey 的消息按序处理。
     * 在多实例部署情况下，同一 Queue 的消息只会被一个实例消费。
     * 
     * @param message 消息内容（JSON 格式字符串）
     */
    @Override
    public void onMessage(String message) {
        logger.info("收到顺序消息: {}", message);
    }
}