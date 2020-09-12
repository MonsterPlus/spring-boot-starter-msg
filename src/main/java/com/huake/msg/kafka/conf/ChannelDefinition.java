package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class ChannelDefinition {
	private String channelId;
	private KafkaConsumerProperties consumerChannel;
	private KafkaProducerProperties producerChannel;
}