package org.tech.rocketmq.controller;

import org.tech.rocketmq.dto.MessageRequest;
import org.tech.rocketmq.dto.MessageResponse;
import org.tech.rocketmq.entity.MessageRecord;
import org.tech.rocketmq.repository.MessageRecordRepository;
import org.tech.rocketmq.service.ProducerService;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 消息控制器（REST API）
 * 
 * <p>提供 RocketMQ 消息发送和消息记录查询的 RESTful API 接口。
 * 所有接口前缀为 /api/message。
 * 
 * <p>消息发送接口：
 * <ul>
 *   <li>POST /sync - 同步消息发送</li>
 *   <li>POST /async - 异步消息发送</li>
 *   <li>POST /oneway - 单向消息发送</li>
 *   <li>POST /delay - 延迟消息发送</li>
 *   <li>POST /orderly - 顺序消息发送</li>
 *   <li>POST /batch - 批量消息发送</li>
 *   <li>POST /transaction - 事务消息发送</li>
 *   <li>POST /tag-filter - 带 Tag 过滤的消息发送</li>
 *   <li>POST /manual-ack - 手动确认消息发送</li>
 *   <li>POST /concurrent - 并发消费测试消息发送</li>
 *   <li>POST /single-thread - 单线程消费测试消息发送</li>
 * </ul>
 * 
 * <p>消息查询接口：
 * <ul>
 *   <li>GET /{messageId} - 根据消息 ID 查询</li>
 *   <li>GET /list - 查询所有消息</li>
 *   <li>GET /status/{status} - 根据状态查询</li>
 *   <li>DELETE /{messageId} - 删除消息记录</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/message")
public class MessageController {

    private static final Logger logger = LoggerFactory.getLogger(MessageController.class);

    @Autowired
    private ProducerService producerService;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Autowired
    private MessageRecordRepository messageRecordRepository;

    /**
     * 同步发送消息
     * 
     * <p>发送后等待 Broker 确认，返回发送结果。适用于需要确保消息发送成功的场景。
     * 
     * @param request 消息请求体，包含 topic、tags、content、messageType
     * @return 包含 messageId 和状态的响应
     */
    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> sendSyncMessage(@RequestBody MessageRequest request) {
        logger.info("send sync: {}", request);
        String messageId = producerService.sendSyncMessage(request);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "sync message sent");
        return ResponseEntity.ok(response);
    }

    /**
     * 异步发送消息
     * 
     * <p>发送后立即返回，通过回调函数获取发送结果。适用于对响应时间要求较高的场景。
     * 
     * @param request 消息请求体，包含 topic、tags、content、messageType
     * @return 包含 messageId 和状态的响应
     */
    @PostMapping("/async")
    public ResponseEntity<Map<String, Object>> sendAsyncMessage(@RequestBody MessageRequest request) {
        logger.info("send async: {}", request);
        String messageId = producerService.sendAsyncMessage(request);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "async message sent");
        return ResponseEntity.ok(response);
    }

    /**
     * 单向发送消息
     * 
     * <p>发送即忘，不等待 Broker 确认，也不返回结果。适用于对可靠性要求不高的场景。
     * 
     * @param request 消息请求体，包含 topic、tags、content、messageType
     * @return 包含 messageId 和状态的响应
     */
    @PostMapping("/oneway")
    public ResponseEntity<Map<String, Object>> sendOnewayMessage(@RequestBody MessageRequest request) {
        logger.info("send oneway: {}", request);
        String messageId = producerService.sendOnewayMessage(request);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "oneway message sent");
        return ResponseEntity.ok(response);
    }

    /**
     * 延迟发送消息
     * 
     * <p>消息发送后不会立即投递，等待指定秒数后再投递。适用于订单超时取消等场景。
     * 
     * @param request      消息请求体，包含 topic、tags、content、messageType
     * @param delaySeconds 延迟秒数，默认 30 秒
     * @return 包含 messageId、延迟时间和状态的响应
     */
    @PostMapping("/delay")
    public ResponseEntity<Map<String, Object>> sendDelayMessage(@RequestBody MessageRequest request, @RequestParam(defaultValue = "30") int delaySeconds) {
        logger.info("send delay: {}, delay: {}s", request, delaySeconds);
        String messageId = producerService.sendDelayMessage(request, delaySeconds);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "delay message sent");
        response.put("delaySeconds", delaySeconds);
        return ResponseEntity.ok(response);
    }

    /**
     * 顺序发送消息
     * 
     * <p>相同 shardingKey 的消息会被发送到同一个队列，保证顺序消费。
     * 适用于订单状态变更、流水日志等需要保证顺序的场景。
     * 
     * @param request     消息请求体，包含 topic、tags、content、messageType
     * @param shardingKey 分片键，相同值的消息保证顺序，默认 "default"
     * @return 包含 messageId、shardingKey 和状态的响应
     */
    @PostMapping("/orderly")
    public ResponseEntity<Map<String, Object>> sendOrderlyMessage(@RequestBody MessageRequest request, @RequestParam(defaultValue = "default") String shardingKey) {
        logger.info("send orderly: {}, shardingKey: {}", request, shardingKey);
        String messageId = producerService.sendOrderlyMessage(request, shardingKey);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "orderly message sent");
        response.put("shardingKey", shardingKey);
        return ResponseEntity.ok(response);
    }

    /**
     * 批量发送消息
     * 
     * <p>将多条消息打包发送，减少网络开销，提高吞吐量。
     * 
     * @param topic    消息主题
     * @param contents 消息内容列表
     * @return 包含发送数量和状态的响应
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatchMessage(@RequestParam String topic, @RequestBody List<String> contents) {
        logger.info("send batch: topic={}, count={}", topic, contents.size());
        producerService.sendBatchMessage(topic, contents);
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "batch message sent");
        response.put("count", contents.size());
        response.put("topic", topic);
        return ResponseEntity.ok(response);
    }

    /**
     * 发送事务消息
     * 
     * <p>事务消息保证本地事务和消息发送的原子性。发送后会执行本地事务，
     * 根据本地事务结果决定提交或回滚消息。
     * 
     * @param request 消息请求体，包含 topic、tags、content、messageType
     * @return 包含 messageId 和状态的响应
     */
    @PostMapping("/transaction")
    public ResponseEntity<Map<String, Object>> sendTransactionMessage(@RequestBody MessageRequest request) {
        logger.info("send transaction: {}", request);
        String messageId = UUID.randomUUID().toString();
        String body = String.format("{\"messageId\":\"%s\",\"content\":\"%s\",\"messageType\":\"TRANSACTION\"}", messageId, request.getContent());
        org.springframework.messaging.Message<String> message = org.springframework.messaging.support.MessageBuilder.withPayload(body).build();
        rocketMQTemplate.sendMessageInTransaction("demo-transaction-producer-group", message, null);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "transaction message sent");
        return ResponseEntity.ok(response);
    }

    /**
     * 根据消息 ID 查询消息记录
     * 
     * @param messageId 消息唯一标识
     * @return 消息响应对象，未找到返回 404
     */
    @GetMapping("/{messageId}")
    public ResponseEntity<MessageResponse> getMessageById(@PathVariable String messageId) {
        return messageRecordRepository.findByMessageId(messageId).map(this::convertToResponse).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    /**
     * 查询所有消息记录
     * 
     * @return 消息响应列表
     */
    @GetMapping("/list")
    public ResponseEntity<List<MessageResponse>> getAllMessages() {
        List<MessageResponse> messages = messageRecordRepository.findAll().stream().map(this::convertToResponse).collect(Collectors.toList());
        return ResponseEntity.ok(messages);
    }

    /**
     * 根据状态查询消息记录
     * 
     * @param status 消息状态（PENDING/CONSUMED/FAILED/DEAD）
     * @return 消息响应列表
     */
    @GetMapping("/status/{status}")
    public ResponseEntity<List<MessageResponse>> getMessagesByStatus(@PathVariable String status) {
        List<MessageResponse> messages = messageRecordRepository.findByStatus(status).stream().map(this::convertToResponse).collect(Collectors.toList());
        return ResponseEntity.ok(messages);
    }

    /**
     * 删除消息记录
     * 
     * @param messageId 消息唯一标识
     * @return 删除结果，未找到返回 404
     */
    @DeleteMapping("/{messageId}")
    public ResponseEntity<Map<String, Object>> deleteMessage(@PathVariable String messageId) {
        return messageRecordRepository.findByMessageId(messageId).map(record -> {
            messageRecordRepository.delete(record);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("message", "message deleted");
            return ResponseEntity.ok(response);
        }).orElse(ResponseEntity.notFound().build());
    }

    /**
     * 将 MessageRecord 实体转换为 MessageResponse DTO
     * 
     * @param record 消息记录实体
     * @return 消息响应 DTO
     */
    private MessageResponse convertToResponse(MessageRecord record) {
        MessageResponse response = new MessageResponse();
        response.setId(record.getId());
        response.setMessageId(record.getMessageId());
        response.setTopic(record.getTopic());
        response.setMessageType(record.getMessageType());
        response.setContent(record.getContent());
        response.setStatus(record.getStatus());
        response.setCreateTime(record.getCreateTime());
        response.setUpdateTime(record.getUpdateTime());
        return response;
    }

    /**
     * 发送带 Tag 过滤的消息
     * 
     * <p>用于测试 Tag 过滤功能，只有匹配 selectorExpression 的消费者才能收到消息。
     * 
     * @param tag     消息标签
     * @param content 消息内容
     * @return 包含 messageId、tag 和状态的响应
     */
    @PostMapping("/tag-filter")
    public ResponseEntity<Map<String, Object>> sendTagFilterMessage(@RequestParam String tag, @RequestParam String content) {
        logger.info("send tag-filter: tag={}, content={}", tag, content);
        String messageId = producerService.sendMessageWithTag("tag-topic", tag, content);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "tag-filter message sent");
        response.put("tag", tag);
        return ResponseEntity.ok(response);
    }

    /**
     * 发送需要手动确认的消息
     * 
     * <p>用于测试手动确认消息消费结果的功能，消费者可以返回 CONSUME_SUCCESS 或 RECONSUME_LATER。
     * 
     * @param content 消息内容
     * @return 包含 messageId 和状态的响应
     */
    @PostMapping("/manual-ack")
    public ResponseEntity<Map<String, Object>> sendManualAckMessage(@RequestParam String content) {
        logger.info("send manual-ack: content={}", content);
        String messageId = producerService.sendMessageToTopic("manual-ack-topic", content);
        Map<String, Object> response = new HashMap<>();
        response.put("messageId", messageId);
        response.put("status", "SUCCESS");
        response.put("message", "manual-ack message sent");
        return ResponseEntity.ok(response);
    }

    /**
     * 发送用于并发消费测试的消息
     * 
     * <p>批量发送指定数量的消息到 concurrent-topic，用于测试多线程并发消费效果。
     * 
     * @param count 消息数量，默认 10
     * @return 包含发送数量和状态的响应
     */
    @PostMapping("/concurrent")
    public ResponseEntity<Map<String, Object>> sendConcurrentMessages(@RequestParam(defaultValue = "10") int count) {
        logger.info("send concurrent: count={}", count);
        producerService.sendMessagesToTopic("concurrent-topic", count);
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "concurrent messages sent");
        response.put("count", count);
        return ResponseEntity.ok(response);
    }

    /**
     * 发送用于单线程消费测试的消息
     * 
     * <p>批量发送指定数量的消息到 single-thread-topic，用于测试单线程顺序消费效果。
     * 
     * @param count 消息数量，默认 5
     * @return 包含发送数量和状态的响应
     */
    @PostMapping("/single-thread")
    public ResponseEntity<Map<String, Object>> sendSingleThreadMessages(@RequestParam(defaultValue = "5") int count) {
        logger.info("send single-thread: count={}", count);
        producerService.sendMessagesToTopic("single-thread-topic", count);
        Map<String, Object> response = new HashMap<>();
        response.put("status", "SUCCESS");
        response.put("message", "single-thread messages sent");
        response.put("count", count);
        return ResponseEntity.ok(response);
    }
}