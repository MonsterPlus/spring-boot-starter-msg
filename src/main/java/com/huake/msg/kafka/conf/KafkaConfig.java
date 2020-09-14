package com.huake.msg.kafka.conf;

import com.huake.msg.kafka.utils.KafkaUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * spring自动配置类，负责初始化kafka相关配置及为KafkaUtils中的配置赋值
 * @Author zkj_95@163.com
 *
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.boot.kafka", name = "enabled", matchIfMissing = false, havingValue = "true")
public class KafkaConfig {

	@Bean
	@ConfigurationProperties(prefix = "spring.boot.kafka.cfg", ignoreUnknownFields = true)
	public ChannelDefinitionConfig setChannelDefinitionConfig() {
		ChannelDefinitionConfig channelDefinitionConfig = new ChannelDefinitionConfig();
		KafkaUtils.setConfig(channelDefinitionConfig);
		return channelDefinitionConfig;
	}
}
