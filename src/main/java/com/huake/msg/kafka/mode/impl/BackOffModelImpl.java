package com.huake.msg.kafka.mode.impl;

import com.huake.msg.kafka.mode.RetryModel;

import java.util.Random;

public class BackOffModelImpl implements RetryModel<Integer> {
	static final double CONTENTION_PERIOD = 51.20;
	static Integer RETRY = 10;

	/**
	 * 
	 */
	public double backOffTime(Integer retry) {
		return getRand(retry) * CONTENTION_PERIOD;
	}

	/**
	 * 获取离散随机整型数字
	 * 
	 * @param retry 重试次数
	 * @return
	 */
	private static int getRand(Integer retry) {
		Integer r = null;
		if (retry <= 0) {
			retry = RETRY;
		}
		Random random = new Random();
		r = random.nextInt((1 << retry) - 0 + 1) + 0;
		return r;
	}
}
