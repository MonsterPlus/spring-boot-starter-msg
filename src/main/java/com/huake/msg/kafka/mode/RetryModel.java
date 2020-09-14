package com.huake.msg.kafka.mode;

/**
 * 重试模型
 * @Author zkj_95@163.com
 */
public interface RetryModel<T> {
	/**
	 *
	 * 重试时间间隔计算方法
	 *
	 * @Description: 重试时间间隔计算方法
	 * @Author: zkj
	 * @Date: 2020/9/14 11:37
	 * @param t: 重试间隔计算所需外在参数
	 * @return: double
	 **/
	public double backOffTime(T t);
}
