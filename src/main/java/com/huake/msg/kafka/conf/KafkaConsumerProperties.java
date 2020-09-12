package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class KafkaConsumerProperties {

	private String topics;// 定义主题

	private String bootstrapServers;

	private Long pollMs = 10000L; // 轮询间隔

	private String groupId;

	private String enableAutoCommit = "true";

	private String autoCommitIntervalMs = "1000";

	private String autoOffsetReset = "latest";

	private String sessionTimeoutMs = "30000";

	private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

	private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
}
