package com.huake.msg.kafka.utils;

import com.huake.msg.kafka.mode.RetryCallback;
import com.huake.msg.kafka.mode.RetryMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class RetryUtil {
	static final AtomicInteger currentTime = new AtomicInteger(0);
	private final static Logger LOGGER = LoggerFactory.getLogger(RetryUtil.class);

	/**
	 * 采用退避算法重传
	 * 
	 * @param retry
	 */
	public static <Req, Res> Res resend(int retry, RetryMode mode, Req request, RetryCallback<Req, Res> callback) {
		Res res = null;
		while (retry > currentTime.get()) {
			try {
				currentTime.getAndAdd(1);
				return callback.send(request);
			} catch (Exception e) {
				try {
					Thread.sleep((long) (mode.backOffTime(retry) / 1000));
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
				LOGGER.info("发送请求时发生异常[{}]，准备第{}次重试,  ", e.getMessage(), currentTime.get());
			}
		}
		LOGGER.info("请求未完成，执行任务失败");
		callback.faild(request);
		return res;
	}
}
