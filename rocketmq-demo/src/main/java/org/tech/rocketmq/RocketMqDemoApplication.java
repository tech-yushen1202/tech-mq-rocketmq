package org.tech.rocketmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * RocketMQ Demo 应用启动类
 * 
 * <p>这是整个 RocketMQ 演示项目的入口类，基于 Spring Boot 3.2.x 构建。
 * 项目演示了 RocketMQ 的多种核心功能，包括：
 * <ul>
 *   <li>同步/异步/单向消息发送</li>
 *   <li>延迟消息</li>
 *   <li>顺序消息</li>
 *   <li>批量消息</li>
 *   <li>事务消息</li>
 *   <li>Tag 过滤消息</li>
 *   <li>手动确认消息</li>
 *   <li>单线程/并发消费</li>
 * </ul>
 * 
 * <p>默认使用 H2 内存数据库进行消息记录存储，也支持切换到 MySQL。
 */
@SpringBootApplication
public class RocketMqDemoApplication {

    /**
     * 应用主入口方法
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        SpringApplication.run(RocketMqDemoApplication.class, args);
    }
}