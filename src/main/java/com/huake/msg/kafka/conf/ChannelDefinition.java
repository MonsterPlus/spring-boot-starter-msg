package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

/**
 * 渠道定义实体类
 * @Author zkj_95@163.com
 */
@Data
@ToString
public class ChannelDefinition {
	private String channelId;
	private KafkaConsumerProperties consumerChannel;
	private KafkaProducerProperties producerChannel;
}