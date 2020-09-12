package com.huake.msg.kafka.callback.impl;


import cn.hutool.json.JSON;
import cn.hutool.json.JSONUtil;
import com.huake.msg.kafka.callback.RetryCallback;
import com.huake.msg.kafka.mode.MessageModel;
import com.huake.msg.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
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
public class DefaultRetryCallbackImpl<Res> implements RetryCallback<ProducerRecord,Res> {
	private final static Logger LOGGER = LoggerFactory.getLogger(DefaultRetryCallbackImpl.class);

	// 初始化队列大小默认10
	private Integer queueSize = 10;
	// 按 topic
	private final Map<String, Map<String, FileWriter>> msgMap = new ConcurrentHashMap(60);

	// 用于存储发送失败的消息
	private Map<String,BlockingQueue<ProducerRecord<?, ?>>> queueMap =new ConcurrentHashMap<>(60);

	// 队列初始化开关
	private static final AtomicBoolean newQuequeSwitch = new AtomicBoolean(false);

	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public Res send(ProducerRecord producerRecord) throws ExecutionException {
		//KafkaProducer kafkaProducer = KafkaUtils.buildKafkaProducer(null);
		//kafkaProducer.send(producerRecord,new ProducerAckCallback(System.currentTimeMillis(), producerRecord));
		KafkaUtils.send(producerRecord.value(),null,null);
		return null;
	}

	@Override
	public <T> T failed(ProducerRecord producerRecord) {
		save(producerRecord, KafkaUtils.getConfig().getChannels().get(0).getChannelId());
		return null;
	}

	public void save(ProducerRecord<?, ?> record, String channelId) {
		lock.lock();
		if (!newQuequeSwitch.get()) {
			newQuequeSwitch.getAndSet(true);
			Integer consumerQueueSize = KafkaUtils.channelSelecter(channelId).getProducerChannel().getQueueSize();
			this.queueSize =consumerQueueSize<1? this.queueSize : consumerQueueSize;
			BlockingQueue<ProducerRecord<?, ?>> producerRecords = Optional.ofNullable(queueMap.get(channelId)).orElseGet(() -> {
				ArrayBlockingQueue<ProducerRecord<?, ?>> channelQueue = new ArrayBlockingQueue<>(this.queueSize);
				queueMap.put(channelId,channelQueue);
				return channelQueue;
			});
		}
		lock.unlock();
		if (!queueMap.get(channelId).offer(record)) {
			queueMap.get(channelId).stream().parallel().forEach(recordItem -> {
				write(record, channelId);
			});
			queueMap.get(channelId).clear();
			queueMap.get(channelId).offer(record);
		}

	}

	public void write(ProducerRecord<?, ?> record, String channelId) {

		final Map<String, FileWriter> writer = Optional.ofNullable(msgMap.get(channelId))
				.orElseGet(() -> createWriter(channelId, record.topic()));
		try {
			//写失败消息到本地文件
			writer.get(channelId).write(record.value() + "\n");
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		msgMap.get(channelId).values().forEach(t -> {
			try {
				t.flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 * 创建不同渠道下以topic区分的FileWriter的map集合
	 * @param channelId 渠道id
	 * @param topic  主题
	 * @return
	 */
	private Map<String, FileWriter> createWriter(String channelId, String topic) {
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
