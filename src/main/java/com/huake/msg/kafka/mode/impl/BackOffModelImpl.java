package com.huake.msg.kafka.mode.impl;

import com.huake.msg.kafka.mode.RetryModel;
import org.springframework.core.annotation.Order;

import java.util.Random;

/**
 * 重试模型退避算法实现
 * @Author zkj_95@163.com
 */
@Order(100)
public class BackOffModelImpl implements RetryModel<Integer> {
	/**
	 * 原子时间粒度
	 */
	static final double CONTENTION_PERIOD = 51.20;
	/**
	 * 默认重试次数
	 */
	static Integer RETRY = 10;

	/**
	 * 重试时间间隔计算方法
	 *
	 * @Description: 重试时间间隔计算方法
	 * @Author: zkj
	 * @Date: 2020/9/14 11:36
	 * @param retry: 重试次数
	 * @return: double
	 **/
	@Override
	public double backOffTime(Integer retry) {
		return getRand(retry) * CONTENTION_PERIOD;
	}

	/**
	 * 	获取离散随机整型数字
	 * @Description: 获取离散随机整型数字
	 * @Author: zkj
	 * @Date: 2020/9/14 11:35
	 * @param retry: 重试次数
	 * @return: java.lang.Integer
	 **/
	private static Integer getRand(Integer retry) {
		Integer r = null;
		if (retry <= 0) {
			retry = RETRY;
		}
		Random random = new Random();
		r = random.nextInt((1 << retry) - 0 + 1) + 0;
		return r;
	}
}
