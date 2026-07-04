package org.tech.rocketmq.repository;

import org.tech.rocketmq.entity.MessageRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 消息记录数据访问层（Repository）
 * 
 * <p>继承 JpaRepository 接口，提供基本的 CRUD 操作，并定义自定义查询方法。
 * Spring Data JPA 会自动根据方法名生成 SQL 查询。
 */
@Repository
public interface MessageRecordRepository extends JpaRepository<MessageRecord, Long> {

    /**
     * 根据消息 ID 查询消息记录
     * 
     * @param messageId 消息唯一标识
     * @return 消息记录 Optional 对象
     */
    Optional<MessageRecord> findByMessageId(String messageId);

    /**
     * 根据主题查询消息记录列表
     * 
     * @param topic 消息主题
     * @return 消息记录列表
     */
    List<MessageRecord> findByTopic(String topic);

    /**
     * 根据状态查询消息记录列表
     * 
     * @param status 消息状态（PENDING/CONSUMED/FAILED/DEAD）
     * @return 消息记录列表
     */
    List<MessageRecord> findByStatus(String status);

    /**
     * 根据主题和状态查询消息记录列表
     * 
     * @param topic 消息主题
     * @param status 消息状态
     * @return 消息记录列表
     */
    List<MessageRecord> findByTopicAndStatus(String topic, String status);
}