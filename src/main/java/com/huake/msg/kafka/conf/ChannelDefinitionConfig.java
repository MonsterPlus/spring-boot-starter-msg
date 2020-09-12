package com.huake.msg.kafka.conf;

import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString
public class ChannelDefinitionConfig {
	private List<ChannelDefinition> channels;
}