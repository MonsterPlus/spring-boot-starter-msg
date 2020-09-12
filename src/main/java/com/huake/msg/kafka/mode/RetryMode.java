package com.huake.msg.kafka.mode;

public interface RetryMode<T> {
	public double backOffTime(T t);
}
