package com.huake.msg.kafka.mode;

public interface RetryModel<T> {
	public double backOffTime(T t);
}
