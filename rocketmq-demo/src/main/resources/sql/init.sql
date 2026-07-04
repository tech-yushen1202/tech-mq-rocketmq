
CREATE DATABASE IF NOT EXISTS rocketmq_demo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE rocketmq_demo;

DROP TABLE IF EXISTS message_record;

CREATE TABLE message_record (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    message_id VARCHAR(100) NOT NULL UNIQUE COMMENT '消息ID',
    topic VARCHAR(100) NOT NULL COMMENT '消息主题',
    message_type VARCHAR(50) NOT NULL COMMENT '消息类型',
    content TEXT COMMENT '消息内容',
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' COMMENT '消息状态：PENDING-待处理，CONSUMED-已消费',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_message_id (message_id),
    INDEX idx_topic (topic),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息记录表';
