package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class KafkaProducerProperties {

	private String topic;// 定义主题

	private String bootstrapServers;

	private String batchSize = "16384";

	private String lingerMs = "0";

	private String compressionType = "none";

	private String acks = "all";

	private Integer retries = 3;

	private boolean async = true;

	private String requestTimeoutMs = "30000";

	private String keySerializer = "org.apache.kafka.common.serialization.IntegerSerializer";

	private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

	private String msgFailPath;
}
