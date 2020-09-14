package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

/**
 * kafka 消息消费方配置信息
 * @Author zkj_95@163.com
 */
@Data
@ToString
public class KafkaConsumerProperties {
	/**
	 * 定义主题
	 */
	private String topics;

	private String bootstrapServers;
	/**
	 *  轮询间隔
	 */
	private Long pollMs = 10000L;

	private String groupId;

	private String enableAutoCommit = "true";

	private String autoCommitIntervalMs = "1000";

	private String autoOffsetReset = "latest";

	private String sessionTimeoutMs = "30000";

	private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

	private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
}
