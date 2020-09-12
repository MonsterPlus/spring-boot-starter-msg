package com.huake.msg.kafka.mode.impl;


import com.huake.msg.kafka.mode.RetryCallback;
import com.huake.msg.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

@Order(100)
public class ProducerDefaultRetryCallbackImpl<Res> implements RetryCallback<ProducerRecord<?, ?>, Res> {
	private final static Logger LOGGER = LoggerFactory.getLogger(ProducerDefaultRetryCallbackImpl.class);

	// 按 topic
	private final Map<String, Map<String, FileWriter>> msgMap = new ConcurrentHashMap(60);

	// 初始化队列大小默认10
	private Integer queueSize = 10;

	// 用于存储发送失败的消息
	private BlockingQueue<ProducerRecord<?, ?>> queue;

	// 队列初始化开关
	private static final AtomicBoolean newQuequeSwitch = new AtomicBoolean(false);

	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public Res send(ProducerRecord<?, ?> producerRecord) throws ExecutionException {

		KafkaUtils.send(producerRecord.value(), null, null);
		return null;
	}

	@Override
	public <T> T faild(ProducerRecord<?, ?> producerRecord) {
		save(producerRecord, KafkaUtils.getConfig().getChannels().get(0).getChannelId());
		return null;
	}

	public void save(ProducerRecord<?, ?> record, String channelId) {
		if (!newQuequeSwitch.get()) {
			queue = new ArrayBlockingQueue<>(queueSize);
			newQuequeSwitch.getAndSet(true);
		}
		if (!queue.offer(record)) {
			queue.stream().parallel().forEach(recordItem -> {
				write(record, channelId);
			});
			queue.clear();
			queue.offer(record);
		}

	}

	public void write(ProducerRecord<?, ?> record, String channelId) {

		final Map<String, FileWriter> writer = Optional.ofNullable(msgMap.get(channelId))
				.orElseGet(() -> writer(channelId, record.topic()));
		try {
			writer.get(channelId).write(record.value() + "\n");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		msgMap.get(channelId).values().forEach(t -> {
			try {
				t.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private Map<String, FileWriter> writer(String channelId, String topic) {
		// 基础文件路径
		String basdir = KafkaUtils.channelSelecter(channelId).getProducerChannel().getMsgFailPath();
		// 渠道路径
		String msgPath = basdir + File.separator + channelId;
		// 创建目录
		try {
			final File directory = new File(msgPath);
			lock.lock();
			if (!directory.exists()) {
				directory.mkdirs();
			}
			lock.unlock();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// 创建文件

		Map<String, FileWriter> topicMap = Optional.ofNullable(msgMap.get(channelId)).orElseGet(() -> {
			FileWriter writer = null;
			try {
				writer = new FileWriter(new File(msgPath, topic));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			Map<String, FileWriter> item = new ConcurrentHashMap(60);
			item.put(topic, writer);
			return item;
		});
		msgMap.put(channelId, topicMap);
		return topicMap;
	}
}
