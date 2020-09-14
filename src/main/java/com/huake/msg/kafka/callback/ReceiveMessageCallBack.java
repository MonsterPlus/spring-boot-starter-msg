package com.huake.msg.kafka.callback;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * 接收消息回调接口
 * @Author zkj_95@163.com
 * @param <T>
 *
 */
public interface ReceiveMessageCallBack<T extends ConsumerRecord> {
	/**
	 * 消息消费回调处理方法
	 *
	 * @Description: 消息消费回调处理方法
	 * @Author: zkj
	 * @Date: 2020/9/14 11:19
	 * @param message: 消息
	 * @param channelId: 渠道id
	 * @return: boolean 状态
	 **/
	public boolean process(ConsumerRecord message, String channelId);
}
