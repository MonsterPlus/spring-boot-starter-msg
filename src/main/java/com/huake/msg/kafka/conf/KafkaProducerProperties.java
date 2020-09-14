package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

/**
 * kafka 生产方配置信息
 * @Author zkj_95@163.com
 */
@Data
@ToString
public class KafkaProducerProperties {
	/**
	 * 定义主题
	 */
	private String topic;
	/**
	 * kafka服务器端地址:端口
	 */
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

	/**
	 * 失败消息暂存目录l
	 **/
	private String msgFailPath;

	/**
	 * 暂存发送失败消息的队列大小
	 **/
	private Integer queueSize = 3;
}
