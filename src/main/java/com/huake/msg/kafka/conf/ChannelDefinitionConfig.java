package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

import java.util.List;

/**
 * 多渠道配置类
 * @Author zkj_95@163.com
 *
 */
@Data
@ToString
public class ChannelDefinitionConfig {
	private List<ChannelDefinition> channels;
}