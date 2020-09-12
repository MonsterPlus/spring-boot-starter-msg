package com.huake.msg.kafka.utils;

import com.huake.msg.kafka.ReceiveMessageCallBack;
import com.huake.msg.kafka.conf.ChannelDefinition;
import com.huake.msg.kafka.conf.ChannelDefinitionConfig;
import com.huake.msg.kafka.conf.KafkaConsumerProperties;
import com.huake.msg.kafka.conf.KafkaProducerProperties;
import com.huake.msg.kafka.mode.RetryCallback;
import com.huake.msg.kafka.mode.RetryMode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka 工具类
 * 
 * @author zuokejin
 * @date 2020年9月7日
 */
public class KafkaUtils {
	private final static Logger BOOT_LOGGER = LoggerFactory.getLogger(KafkaUtils.class);
	private static ChannelDefinitionConfig config;

	private static final AtomicBoolean consumerSwitch = new AtomicBoolean(false);

	private KafkaUtils() {

	}

	public static void stop() {
		consumerSwitch.getAndSet(false);
	}

	public static void start() {
		consumerSwitch.getAndSet(true);
	}

	public static ChannelDefinitionConfig getConfig() {
		return KafkaUtils.config;
	}

	public static void setConfig(ChannelDefinitionConfig channelDefinitionConfig) {
		KafkaUtils.config = channelDefinitionConfig;
	}

	public static <K, T> KafkaProducer<K, T> buildKafkaProducer(String channelId) {
		configIsNull(config);
		Properties producerProperties = getKafkaProperties(channelId, true);
		KafkaProducer<K, T> kafkaProducer = new KafkaProducer<>(producerProperties);
		return kafkaProducer;
	}

	public static <K, T> KafkaConsumer<K, T> buildKafkaConsumer(String channelId) {
		configIsNull(config);
		Properties consumerProperties = getKafkaProperties(channelId, false);
		KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		return kafkaConsumer;
	}

	public static Properties getKafkaProperties(String channelId, Boolean isProducer) {
		configIsNull(config);
		Properties properties = null;
		if (isProducer) {
			properties = buildProducerProperties(channelId);
		} else {
			properties = buildConsumerProperties(channelId);
		}
		return properties;
	}

	public static <T> void send(int retry, RetryMode mode, String topic, T msg) {
		ProducerRecord<?, ?> record = null;
		if (topic != null && !"".equals(topic)) {
			record = new ProducerRecord(topic, msg);
		} else {
			record = new ProducerRecord(KafkaUtils.channelSelecter(null).getProducerChannel().getTopic(), msg);
		}

		RetryUtil.resend(retry, mode, record, SpringFactoriesUtils.loadFactories(RetryCallback.class));
	}

	/**
	 * 发送kafka消息，采用默认方式
	 * 
	 * @param <K>
	 * @param <T>
	 * @param msg
	 * @throws ExecutionException
	 */
	public static <K, T> void send(T msg, String channelId, String topic) throws ExecutionException {
		configIsNull(config);
		KafkaProducer<K, T> kafkaProducer = buildKafkaProducer(channelId);
		ProducerRecord<K, T> record = null;
		if (topic != null && !"".equals(topic)) {
			record = new ProducerRecord(topic, msg);
		} else {
			record = new ProducerRecord(channelSelecter(channelId).getProducerChannel().getTopic(), msg);
		}

		try {
			sendMsg(msg, kafkaProducer, record, channelId);
		} catch (Exception e) {
			String msgFailPath = channelSelecter(channelId).getProducerChannel().getMsgFailPath();

		} finally {
			kafkaProducer.close();
		}
	}

	/**
	 * kafka 消息发送 ，同步/异步方式
	 * 
	 * @param <T>
	 * @param <K>
	 * @param msg
	 * @param kafkaProducer
	 * @param record
	 * @throws ExecutionException
	 */
	private static <T, K> void sendMsg(T msg, KafkaProducer<K, T> kafkaProducer, ProducerRecord<K, T> record,
			String channleId) throws ExecutionException {
		configIsNull(config);
		KafkaProducerProperties producerProperties = channelSelecter(channleId).getProducerChannel();
		if (producerProperties.isAsync()) {
			kafkaProducer.send(record, new ProducerAckCallback(System.currentTimeMillis(), msg));
		} else {
			try {
				/** 同步等待返回 */
				kafkaProducer.send(record).get();
				BOOT_LOGGER.debug("Send msg:{%s}", msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 订阅式拉取消息内容
	 * 
	 * @param <K>
	 * @param <T>
	 * @param channleId 渠道id
	 * @return
	 */
	public static <K, T> List<ConsumerRecord<K, T>> recordForSubscribe(String channleId) {
		configIsNull(config);
		KafkaConsumerProperties consumerProperties = channelSelecter(channleId).getConsumerChannel();
		List<ConsumerRecord<K, T>> recordList = new ArrayList<>();
		KafkaConsumer<K, T> kafkaConsumer = buildKafkaConsumer(channleId);
		kafkaConsumer.subscribe(paresList(consumerProperties.getTopics()));
		kafkaConsumer.poll(Duration.ofMillis(consumerProperties.getPollMs())).forEach(record -> {
			recordList.add(record);
		});
		return recordList;
	}

	public static <K, T> List<ConsumerRecord<K, T>> recordForAssign(String channleId) {
		configIsNull(config);
		KafkaConsumerProperties consumerProperties = channelSelecter(channleId).getConsumerChannel();
		KafkaConsumer<K, T> consumer = buildKafkaConsumer(channleId);
		List<TopicPartition> topics = new ArrayList<>();
		paresList(consumerProperties.getTopics()).forEach(topic -> {
			TopicPartition tp = new TopicPartition(topic, 0);
			topics.add(tp);
		});
		consumer.assign(topics);
		List<ConsumerRecord<K, T>> buffer = new ArrayList<>();
		consumer.poll(Duration.ofMillis(consumerProperties.getPollMs())).forEach(record -> {
			buffer.add(record);
		});
		consumer.commitSync();
		return buffer;
	}

	public static <K, T> void consumerClient(String channleId) {
		configIsNull(config);
		KafkaConsumerProperties consumerProperties = channelSelecter(channleId).getConsumerChannel();
		KafkaConsumer<K, T> kafkaConsumer = buildKafkaConsumer(channleId);
		kafkaConsumer.subscribe(paresList(consumerProperties.getTopics()));
		ReceiveMessageCallBack bean = SpringFactoriesUtils.loadFactories(ReceiveMessageCallBack.class);

		kafkaConsumer.poll(Duration.ofMillis(consumerProperties.getPollMs())).forEach(record -> {
			bean.process(record, channleId, record.topic());
		});
	}

	private static void configIsNull(ChannelDefinitionConfig config) {
		if (config == null) {
			throw new RuntimeException("kafka 配置未开启");
		}
	}

	private static Properties buildConsumerProperties(String channelId) {
		Properties properties = new Properties();
		KafkaConsumerProperties consumerProperties = channelSelecter(channelId).getConsumerChannel();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProperties.getBootstrapServers());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.getGroupId());
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumerProperties.getEnableAutoCommit());
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, consumerProperties.getAutoCommitIntervalMs());
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerProperties.getAutoOffsetReset());
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerProperties.getSessionTimeoutMs());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, consumerProperties.getKeyDeserializer());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, consumerProperties.getValueDeserializer());
		return properties;
	}

	private static Properties buildProducerProperties(String channelId) {
		Properties properties = new Properties();
		KafkaProducerProperties producerProperties = channelSelecter(channelId).getProducerChannel();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProperties.getBootstrapServers());// kafka地址，多个地址用逗号分割
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producerProperties.getKeySerializer());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producerProperties.getValueSerializer());
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerProperties.getBatchSize());
		properties.put(ProducerConfig.LINGER_MS_CONFIG, producerProperties.getLingerMs());
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producerProperties.getCompressionType());
		properties.put(ProducerConfig.ACKS_CONFIG, producerProperties.getAcks());
		properties.put(ProducerConfig.RETRIES_CONFIG, producerProperties.getRetries());
		properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerProperties.getRequestTimeoutMs());
		return properties;
	}

	private static List<String> paresList(String topics) {
		List<String> topicList = null;
		if (topics != null && !"".equals(topics)) {
			String[] split = topics.trim().split(",");
			topicList = Arrays.asList(split);
		}
		return topicList;
	}

	/**
	 * 渠道选择器
	 * 
	 * @param channelId
	 * @return
	 */
	public static ChannelDefinition channelSelecter(String channelId) {
		ChannelDefinition channelDefinition = null;
		try {
			if (null != channelId && !"".equals(channelId)) {
				List<ChannelDefinition> channels = config.getChannels();
				for (ChannelDefinition channel : channels) {
					if (channelId.equals(channel.getChannelId())) {
						channelDefinition = channel;
						break;
					}
				}
			} else {
				channelDefinition = config.getChannels().get(0);
			}
			if (null == channelDefinition) {
				throw new RuntimeException("未找到指定 channelId 对应的配置项");
			}

		} catch (Exception e) {
			throw new RuntimeException("kafka 消费端未配置:{}", e);
		}
		return channelDefinition;
	}

	/**
	 * 应答式消息提供者消息发生回调函数，发生成功后回调应答消息 ack开关为true时自动调用
	 * 
	 * @author huake
	 *
	 * @param <T>
	 */
	static class ProducerAckCallback<T> implements org.apache.kafka.clients.producer.Callback {

		private final Long startTime;
		private final T value;

		public ProducerAckCallback(Long startTime, T value) {
			this.startTime = startTime;
			this.value = value;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long spendTime = System.currentTimeMillis() - startTime;
			if (null != metadata) {
				BOOT_LOGGER.debug("消息({%s})send to partition({%s}) and offest {%s} and spend  {%s} ms", value,
						metadata.partition(), metadata.offset(), spendTime);
			}
		}
	}
}
