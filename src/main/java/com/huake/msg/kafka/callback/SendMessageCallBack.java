package com.huake.msg.kafka.callback;


public interface SendMessageCallBack<T> {
	public boolean process(T message);

	public boolean process(T message, String channelId);
}
