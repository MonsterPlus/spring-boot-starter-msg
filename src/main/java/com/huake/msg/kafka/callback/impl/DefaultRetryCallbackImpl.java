package com.huake.msg.kafka.callback.impl;


import com.huake.msg.kafka.callback.RetryCallback;
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

/**
 * 失败重试回调默认实现
 * @author zuokejin
 * @param <Res>
 */
@Order(100)
public class DefaultRetryCallbackImpl<Res> implements RetryCallback<ProducerRecord,Res> {
	private final static Logger LOGGER = LoggerFactory.getLogger(DefaultRetryCallbackImpl.class);

	/**
	 * 初始化队列大小默认10
	 **/
	private Integer queueSize = 10;
	/**
	 * 按 渠道 分组存放不同 topic 的 FileWriter
	 */
	private final Map<String, Map<String, FileWriter>> msgMap = new ConcurrentHashMap(60);

	/**
	 * 用于存储发送失败的消息
	 */
	private Map<String,BlockingQueue<ProducerRecord<?, ?>>> queueMap =new ConcurrentHashMap<>(60);

	/**
	 * 队列初始化开关
	 **/
	private static final AtomicBoolean newQueueSwitch = new AtomicBoolean(false);
	/**
	 * 锁
	 **/
	private final ReentrantLock lock = new ReentrantLock();

	@Override
	public Res send(ProducerRecord producerRecord) throws ExecutionException {
		KafkaUtils.send(producerRecord.value(),null,null);
		return null;
	}

	@Override
	public <T> T failed(ProducerRecord producerRecord) {
		save(producerRecord, KafkaUtils.getConfig().getChannels().get(0).getChannelId());
		return null;
	}

	private void save(ProducerRecord<?, ?> record, String channelId) {
		lock.lock();
		try {
			if (!newQueueSwitch.get()) {
				newQueueSwitch.getAndSet(true);
				Integer consumerQueueSize = KafkaUtils.channelSelector(channelId).getProducerChannel().getQueueSize();
				this.queueSize =consumerQueueSize<1? this.queueSize : consumerQueueSize;
				BlockingQueue<ProducerRecord<?, ?>> producerRecords = Optional.ofNullable(queueMap.get(channelId)).orElseGet(() -> {
					ArrayBlockingQueue<ProducerRecord<?, ?>> channelQueue = new ArrayBlockingQueue<>(this.queueSize);
					queueMap.put(channelId,channelQueue);
					return channelQueue;
				});
			}
		}finally {
			lock.unlock();
		}
		if (!queueMap.get(channelId).offer(record)) {
			queueMap.get(channelId).stream().parallel().forEach(recordItem -> {
				write(record, channelId);
			});
			queueMap.get(channelId).clear();
			queueMap.get(channelId).offer(record);
		}

	}

	/**
	 * 向本地目录写最终失败的消息内容
	 *
	 * @param record
	 * @param channelId 渠道id
	 */
	private void write(ProducerRecord<?, ?> record, String channelId) {

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
		String basDir = KafkaUtils.channelSelector(channelId).getProducerChannel().getMsgFailPath();
		// 渠道路径
		String msgPath = basDir + File.separator + channelId;
		// 创建目录
		final File directory = new File(msgPath);
		lock.lock();
		try {
			if (!directory.exists()) {
				directory.mkdirs();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally {
			lock.unlock();
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
