package com.huake.msg.kafka.utils;

import cn.hutool.json.JSONUtil;
import com.huake.msg.kafka.callback.ReceiveMessageCallBack;
import com.huake.msg.kafka.callback.RetryCallback;
import com.huake.msg.kafka.category.ConsumptionCategory;
import com.huake.msg.kafka.conf.ChannelDefinition;
import com.huake.msg.kafka.conf.ChannelDefinitionConfig;
import com.huake.msg.kafka.conf.KafkaConsumerProperties;
import com.huake.msg.kafka.conf.KafkaProducerProperties;
import com.huake.msg.kafka.mode.MessageModel;
import com.huake.msg.kafka.mode.RetryModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * kafka 工具类
 *
 * @Author zkj_95@163.com
 * @date 2020年9月7日
 */
public class KafkaUtils {
	private final static Logger BOOT_LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

	private static ChannelDefinitionConfig config;

	private KafkaUtils() {}

	public static ChannelDefinitionConfig getConfig() {
		return KafkaUtils.config;
	}

	public static void setConfig(ChannelDefinitionConfig channelDefinitionConfig) {
		KafkaUtils.config = channelDefinitionConfig;
	}

	/**
	 * 获取kafka配置信息对象
	 *
	 * @Description: 获取kafka配置信息对象
	 * @Author: zkj
	 * @Date: 2020/9/14 16:45
	 * @param channelId: 渠道id
	 * @param isProducer: 是否生产者
	 * @return: java.util.Properties
	 **/
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
	/**
	 * 采用重试机制进行消息发送
	 *
	 * @Description:
	 * @Author: zkj
	 * @Date: 2020/9/14 16:37
	 * @param msg: 消息内容
	 * @param mode: 重试模型
	 * @param topic: 主题
	 * @param channelId: 渠道id
	 * @return: void
	 **/
	public static <T extends MessageModel> void resend( T msg, RetryModel mode, String topic,String channelId) {
		ProducerRecord<?, ?> record = null;
		KafkaProducerProperties producerChannel = KafkaUtils.channelSelector(null).getProducerChannel();
		if (topic != null && !"".equals(topic)) {
			record = new ProducerRecord(topic, msg);
		} else {
			record = new ProducerRecord(producerChannel.getTopic(), msg);
		}
		RetryUtil.resend(producerChannel.getRetries(), mode, record, SpringFactoriesUtils.loadFactories(RetryCallback.class));
	}

	/**
	 * 发送kafka消息
	 *
	 * @Description: 发送kafka消息
	 * @Author: zkj
	 * @Date: 2020/9/14 16:35
	 * @param msg: 消息内容
	 * @param channelId: 渠道id
	 * @param topic: 主题
	 * @return: void
	 **/
	public static <K, T> void send(T msg, String channelId, String topic) throws ExecutionException {
		configIsNull(config);
		KafkaProducer<K, T> kafkaProducer = buildKafkaProducer(channelId);
		ProducerRecord<K, T> record = null;
		String jsonStr = JSONUtil.toJsonStr(msg);
		KafkaProducerProperties producerChannel = channelSelector(channelId).getProducerChannel();
		if (topic != null && !"".equals(topic)) {
			record = new ProducerRecord(topic, jsonStr);
		} else {
			record = new ProducerRecord(producerChannel.getTopic(), jsonStr);
		}
		try {
			sendMsg(producerChannel,kafkaProducer, record, channelId);
		} catch (Exception e) {
			String msgFailPath = producerChannel.getMsgFailPath();

		} finally {
			kafkaProducer.close();
		}
	}

	/**
	 * kafka 消息发送 ，同步/异步方式
	 *
	 * @Description: kafka 消息发送 ，同步/异步方式
	 * @Author: zkj
	 * @Date: 2020/9/14 16:28
	 * @param kafkaProducer:
	 * @param record:
	 * @param channelId:
	 * @return: void
	 **/
	private static <T, K> void sendMsg(KafkaProducerProperties producerProperties,KafkaProducer<K, T> kafkaProducer, ProducerRecord<K, T> record,
			String channelId) throws ExecutionException {
		if (producerProperties.isAsync()) {
			Callback callback=null;
			try {
				callback = SpringFactoriesUtils.loadFactories(Callback.class);
			}catch (Exception e){
				throw new RuntimeException(e);
			}
			kafkaProducer.send(record, callback);
		} else {
			try {
				/** 同步等待返回 */
				kafkaProducer.send(record).get();
				BOOT_LOGGER.debug("Send msg:{%s}", record.value());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 消息消费者api
	 *
	 * @Description: 消息消费者api
	 * @Author: zkj
	 * @Date: 2020/9/14 14:06
	 * @param channelId: 渠道id
	 * @return: void
	 **/
	public static <K, T> void consumerClient(String channelId, ConsumptionCategory category) {
		configIsNull(config);
		KafkaConsumerProperties consumerProperties = channelSelector(channelId).getConsumerChannel();
		KafkaConsumer<K, T> kafkaConsumer = buildKafkaConsumer(channelId);
		ReceiveMessageCallBack bean = SpringFactoriesUtils.loadFactories(ReceiveMessageCallBack.class);
		switch (category){
			case ASSIGN:
				List<TopicPartition> topics = new ArrayList<>();
				paresList(consumerProperties.getTopics()).forEach(topic -> {
					TopicPartition tp = new TopicPartition(topic, 0);
					topics.add(tp);
				});
				kafkaConsumer.assign(topics);
				kafkaConsumer.poll(Duration.ofMillis(consumerProperties.getPollMs())).forEach(record -> {
					bean.process(record,channelId);
				});
				kafkaConsumer.commitSync();
				break;
			case SUBSCRIBE:
				kafkaConsumer.subscribe(paresList(consumerProperties.getTopics()));
				kafkaConsumer.poll(Duration.ofMillis(consumerProperties.getPollMs())).forEach(record -> {
					bean.process(record,channelId);
				});
				break;
			default:
				break;
		}
	}
	/**
	 * kafka工具类使用合法性校验
	 *
	 * @Description: kafka工具类使用合法性校验
	 * @Author: zkj
	 * @Date: 2020/9/14 14:05
	 * @param config: kafka配置信息
	 * @return: void
	 **/
	private static void configIsNull(ChannelDefinitionConfig config) {
		if (config == null) {
			throw new RuntimeException("kafka 配置未开启");
		}
	}
	/**
	 * 构建kafka 生产者对象
	 * @Description:
	 * @Author: zkj
	 * @Date: 2020/9/14 16:47
	 * @param channelId: 渠道id
	 * @return: org.apache.kafka.clients.producer.KafkaProducer<K,T>
	 **/
	public static <K, T> KafkaProducer<K, T> buildKafkaProducer(String channelId) {
		configIsNull(config);
		Properties producerProperties = getKafkaProperties(channelId, true);
		KafkaProducer<K, T> kafkaProducer = new KafkaProducer<>(producerProperties);
		return kafkaProducer;
	}
	/**
	 * 构建kafka消费者对象
	 *
	 * @Description: 构建kafka消费者对象
	 * @Author: zkj
	 * @Date: 2020/9/14 16:45
	 * @param channelId:  渠道id
	 * @return: org.apache.kafka.clients.consumer.KafkaConsumer<K,T>
	 **/
	public static <K, T> KafkaConsumer<K, T> buildKafkaConsumer(String channelId) {
		configIsNull(config);
		Properties consumerProperties = getKafkaProperties(channelId, false);
		KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		return kafkaConsumer;
	}
	/**
	 * 构建消费者properties配置对象
	 * @Description:
	 * @Author: zkj
	 * @Date: 2020/9/14 14:04
	 * @param channelId: 渠道id
	 * @return: java.util.Properties
	 **/
	private static Properties buildConsumerProperties(String channelId) {
		Properties properties = new Properties();
		KafkaConsumerProperties consumerProperties = channelSelector(channelId).getConsumerChannel();
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
	/**
	 * 构建消息生产者properties配置对象
	 *
	 * @Description: 构建消息生产者properties配置对象
	 * @Author: zkj
	 * @Date: 2020/9/14 14:02
	 * @param channelId: 渠道id
	 * @return: java.util.Properties
	 **/
	private static Properties buildProducerProperties(String channelId) {
		Properties properties = new Properties();
		KafkaProducerProperties producerProperties = channelSelector(channelId).getProducerChannel();
		// kafka地址，多个地址用逗号分割
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProperties.getBootstrapServers());
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
	/**
	 * 主题数据解析
	 * 对以","分割的topic字符串进行解析，将其转换为list集合
	 * @Description: 主题数据解析
	 * @Author: zkj
	 * @Date: 2020/9/14 14:00
	 * @param topics: 主题列表
	 * @return: java.util.List<java.lang.String>
	 **/
	private static List<String> paresList(String topics) {
		List<String> topicList = null;
		if (topics != null && !"".equals(topics)) {
			String[] split = topics.trim().split(",");
			topicList = Arrays.asList(split);
		}
		return topicList;
	}

	/**
	 * 渠道选择器,根据渠道id查找该id对应的配置信息
	 *
	 * @Description: 渠道选择器,根据渠道id查找该id对应的配置信息
	 * @Author: zkj
	 * @Date: 2020/9/14 11:40
	 * @param channelId: 渠道id
	 * @return: com.huake.msg.kafka.conf.ChannelDefinition
	 **/
	public static ChannelDefinition channelSelector(String channelId) {
		ChannelDefinition channelDefinition = null;
		try {
			if (null != channelId && !"".equals(channelId)) {
				channelDefinition=config.getChannels().stream().filter((channel) -> channelId.equals(channel.getChannelId())).findFirst().get();
			} else {
				//默认取第一个渠道
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
}
