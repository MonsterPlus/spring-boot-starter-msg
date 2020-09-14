package com.huake.msg.kafka.callback;

import java.util.concurrent.ExecutionException;

/**
 * 回调接口方法
 *
 * @Author zkj_95@163.com
 * @param <Req extends MessageModel>
 * @param <Res>
 */
public interface RetryCallback<Req, Res> {
	/**
	 * 发送请求时回调
	 *
	 * @Description: 发送请求时回调
	 * @Author: zkj
	 * @Date: 2020/9/14 11:20
	 * @param request: 请求数据
	 * @return: Res
	 *
	 */
	public Res send(Req request) throws ExecutionException;

	/**
	 * 消息发送失败最终处理方法
	 *
	 * @Description: 消息发送失败最终处理方法
	 * @Author: zkj
	 * @Date: 2020/9/14 11:24
	 * @param request: 请求数据
	 * @return: T
	 **/
	public <T> T failed(Req request);
}