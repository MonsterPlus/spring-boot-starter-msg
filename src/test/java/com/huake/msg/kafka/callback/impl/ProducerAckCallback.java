package com.huake.msg.kafka.callback.impl;

import com.huake.msg.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;

/**
 * 应答式消息提供者消息发生回调函数，发生成功后回调应答消息 ack开关为true时自动调用
 *
 * @author zuokejin
 *
 * @param <T>
 */
@Order(100)
public class ProducerAckCallback<T extends ProducerRecord> implements Callback {

	private final static Logger BOOT_LOGGER = LoggerFactory.getLogger(KafkaUtils.class);
	//开始发送时间
	private  Long startTime;

	public ProducerAckCallback(){
		startTime=System.currentTimeMillis();
	}
	public ProducerAckCallback(Long startTime){
		startTime=startTime;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long spendTime = System.currentTimeMillis() - startTime;
		if (null != metadata) {
			BOOT_LOGGER.debug("message send to partition({%s}) and offest {%s} and spend  {%s} ms",
					metadata.partition(), metadata.offset(), spendTime);
		}
	}
}