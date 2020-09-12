package com.huake.msg.kafka.utils;

import com.huake.msg.kafka.callback.impl.ProducerAckCallback;
import com.huake.msg.kafka.conf.KafkaProducerProperties;
import com.huake.msg.kafka.mode.MessageModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * 对未成功发送的消息的处理
 * 
 * 1、队列 -->用于暂存未成功发送的消息，
 * 
 * 2、如果队列已满，则将队列内容写入到磁盘中
 * 
 * 3、使用退避算法对发送失败的消息进行重发尝试，如果多次尝试无果则记为不可达
 * 
 * 4、将不可达消息保存在不可达文件中
 * 
 * @author zuokejin
 *
 */
public class ProducerCallback {

	private final static Logger BOOT_LOGGER = LoggerFactory.getLogger(ProducerCallback.class);

	// 初始化队列大小
	private Integer queueSize;
	// 用于存储发送失败的消息
	private BlockingQueue<ProducerRecord<?, ?>> queue;

	public ProducerCallback(Integer queueSize) {
		this.queueSize = queueSize;
		queue = new ArrayBlockingQueue<>(queueSize);
	}

	public void save(ProducerRecord<?, ?> record) {
		queue.offer(record);
	}

	public void write(String path, String fileName) {
		try {
			final PrintWriter writer = new PrintWriter(new File(path, fileName));

		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public <T, K> void send(T msg, KafkaProducer<K, T> kafkaProducer, ProducerRecord<K, T> record, String channleId) {
		KafkaProducerProperties producerProperties = new KafkaProducerProperties();
		if (producerProperties.isAsync()) {
			kafkaProducer.send(record, new ProducerAckCallback(System.currentTimeMillis(), record));
		} else {
			try {
				/** 同步等待返回 */
				kafkaProducer.send(record).get();
				BOOT_LOGGER.debug("Send msg:{%s}", msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

}
