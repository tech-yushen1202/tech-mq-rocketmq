package org.tech.rocketmq.consumer;

import org.tech.rocketmq.entity.MessageRecord;
import org.tech.rocketmq.repository.MessageRecordRepository;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * RocketMQ 事务消息监听器
 * 
 * <p>实现 RocketMQ 的事务消息机制，保证本地事务和消息发送的原子性。
 * 
 * <p>事务消息流程：
 * <ol>
 *   <li>生产者发送半消息到 Broker</li>
 *   <li>Broker 存储半消息并返回确认</li>
 *   <li>生产者执行本地事务（executeLocalTransaction）</li>
 *   <li>根据本地事务结果提交或回滚消息</li>
 *   <li>若超时未收到结果，Broker 主动回查（checkLocalTransaction）</li>
 * </ol>
 * 
 * <p>事务状态说明：
 * <ul>
 *   <li>COMMIT - 提交事务，消息对消费者可见</li>
 *   <li>ROLLBACK - 回滚事务，消息被删除</li>
 *   <li>UNKNOWN - 状态未知，等待回查</li>
 * </ul>
 */
@RocketMQTransactionListener
@Component
public class TransactionListenerImpl implements RocketMQLocalTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListenerImpl.class);

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    /**
     * 执行本地事务
     * 
     * <p>在消息发送成功后执行本地事务逻辑。
     * 将消息记录保存到数据库，如果保存成功则提交事务，否则回滚。
     * 
     * @param msg 半消息内容
     * @param arg 自定义参数（当前未使用）
     * @return 事务状态（COMMIT/ROLLBACK/UNKNOWN）
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        logger.info("执行本地事务: {}", msg.getPayload());
        
        try {
            String payload = (String) msg.getPayload();
            String messageId = payload.contains("messageId") ? 
                    payload.split("messageId\":\"")[1].split("\"")[0] : null;
            
            if (messageId != null) {
                MessageRecord record = MessageRecord.builder()
                        .messageId(messageId)
                        .topic("transaction-topic")
                        .messageType("TRANSACTION")
                        .content(payload)
                        .status("PENDING")
                        .build();
                messageRecordRepository.save(record);
                logger.info("本地事务执行成功 - messageId: {}", messageId);
                return RocketMQLocalTransactionState.COMMIT;
            } else {
                logger.warn("消息ID为空，回滚事务");
                return RocketMQLocalTransactionState.ROLLBACK;
            }
        } catch (Exception e) {
            logger.error("本地事务执行失败", e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    /**
     * 检查本地事务状态（回查）
     * 
     * <p>当 Broker 超时未收到事务提交或回滚结果时，主动调用此方法查询事务状态。
     * 根据数据库中消息记录的状态决定是否提交事务。
     * 
     * @param msg 半消息内容
     * @return 事务状态（COMMIT/ROLLBACK/UNKNOWN）
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        logger.info("检查本地事务状态: {}", msg.getPayload());
        
        try {
            String payload = (String) msg.getPayload();
            String messageId = payload.contains("messageId") ? 
                    payload.split("messageId\":\"")[1].split("\"")[0] : null;
            
            if (messageId != null) {
                return messageRecordRepository.findByMessageId(messageId)
                        .map(record -> {
                            if ("PENDING".equals(record.getStatus())) {
                                return RocketMQLocalTransactionState.COMMIT;
                            }
                            return RocketMQLocalTransactionState.COMMIT;
                        })
                        .orElse(RocketMQLocalTransactionState.ROLLBACK);
            }
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            logger.error("本地事务检查失败", e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }
}