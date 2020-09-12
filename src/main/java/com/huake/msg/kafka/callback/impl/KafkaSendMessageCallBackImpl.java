package com.huake.msg.kafka.callback.impl;

import com.huake.msg.kafka.callback.SendMessageCallBack;
import com.huake.msg.kafka.mode.impl.DefaultMessageModel;
import com.huake.msg.kafka.utils.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;

@Order(100)
public class KafkaSendMessageCallBackImpl implements SendMessageCallBack<DefaultMessageModel> {
	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaSendMessageCallBackImpl.class);

	@Override
	public boolean process(DefaultMessageModel message) {
		try {
			KafkaUtils.send(message, null, null);
			LOGGER.debug("===============消息发送成功=============");
		} catch (Exception e) {
			LOGGER.debug("===============消息发送失败=============");
			LOGGER.debug("主题：{} , 消息服务器：{} 异常信息：{}", "", "", e);
			return false;
		}
		return true;
	}

	@Override
	public boolean process(DefaultMessageModel message, String channelId) {
		// TODO Auto-generated method stub
		return false;
	}
}
