package org.tech.rocketmq.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.LocalDateTime;

/**
 * 消息响应 DTO（Data Transfer Object）
 * 
 * <p>用于返回消息记录的详细信息，包含数据库中存储的消息元数据和业务数据。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageResponse {

    /**
     * 数据库主键 ID
     */
    private Long id;

    /**
     * 消息唯一标识（messageId）
     * <p>由生产者生成的 UUID，用于追踪和查询消息。
     */
    private String messageId;

    /**
     * 消息主题（Topic）
     */
    private String topic;

    /**
     * 消息类型
     * <p>如：SYNC、ASYNC、DELAY、ORDERLY、TRANSACTION、TAG_FILTER、MANUAL_ACK 等。
     */
    private String messageType;

    /**
     * 消息内容
     */
    private String content;

    /**
     * 消息状态
     * <p>PENDING - 待处理；CONSUMED - 已消费；FAILED - 消费失败；DEAD - 死信消息。
     */
    private String status;

    /**
     * 创建时间
     * <p>消息记录入库的时间。
     */
    private LocalDateTime createTime;

    /**
     * 更新时间
     * <p>消息记录最后更新的时间，由 @PreUpdate 自动更新。
     */
    private LocalDateTime updateTime;
}