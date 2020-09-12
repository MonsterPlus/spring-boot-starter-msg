package com.huake.msg.kafka.callback;


public interface ReceiveMessageCallBack<T> {
	public boolean process(T message, String channelId, String topic);
}
