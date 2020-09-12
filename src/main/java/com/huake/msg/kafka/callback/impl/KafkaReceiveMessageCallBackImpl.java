package com.huake.msg.kafka.callback.impl;

import com.huake.msg.kafka.callback.ReceiveMessageCallBack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;

@Order(100)
public class KafkaReceiveMessageCallBackImpl implements ReceiveMessageCallBack<ConsumerRecord> {
	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaReceiveMessageCallBackImpl.class);

	@Override
	public boolean process(ConsumerRecord message, String channelId, String topic) {
		System.out.println("m2 offset : {" + message.offset() + "} , value : {" + message.value() + "},channel :{"
				+ channelId + "} topic : {" + topic + "}");
		LOGGER.debug("m2 offset : {%s} , value : {%s}", message.offset(), message.value());
		return true;
	}

}
