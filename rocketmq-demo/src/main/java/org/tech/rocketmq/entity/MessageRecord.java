package org.tech.rocketmq.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;

/**
 * 消息记录实体类
 * 
 * <p>用于持久化存储消息消费记录，对应数据库表 message_record。
 * 使用 JPA 注解定义实体与数据库表的映射关系。
 */
@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "message_record")
public class MessageRecord {

    /**
     * 主键 ID，自增
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 消息唯一标识
     * <p>由生产者生成的 UUID，在同一个 Topic 内唯一。
     */
    @Column(name = "message_id", nullable = false, unique = true, length = 100)
    private String messageId;

    /**
     * 消息主题（Topic）
     */
    @Column(name = "topic", nullable = false, length = 100)
    private String topic;

    /**
     * 消息类型
     * <p>标识消息的发送方式或业务类型。
     */
    @Column(name = "message_type", nullable = false, length = 50)
    private String messageType;

    /**
     * 消息内容
     * <p>存储消息的原始内容，使用 TEXT 类型支持较长内容。
     */
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;

    /**
     * 消息状态
     * <p>PENDING - 待处理；CONSUMED - 已消费；FAILED - 消费失败；DEAD - 死信消息。
     * 默认值为 PENDING。
     */
    @Column(name = "status", nullable = false, length = 20)
    @Builder.Default
    private String status = "PENDING";

    /**
     * 创建时间
     * <p>消息记录入库的时间，默认值为当前时间。
     */
    @Column(name = "create_time")
    @Builder.Default
    private LocalDateTime createTime = LocalDateTime.now();

    /**
     * 更新时间
     * <p>消息记录最后更新的时间，在更新操作时自动刷新。
     */
    @Column(name = "update_time")
    @Builder.Default
    private LocalDateTime updateTime = LocalDateTime.now();

    /**
     * 更新前自动设置更新时间
     * <p>使用 JPA 的 @PreUpdate 回调，在每次更新实体前自动设置当前时间。
     */
    @PreUpdate
    public void preUpdate() {
        this.updateTime = LocalDateTime.now();
    }
}