package com.huake.msg.kafka.callback;

import com.huake.msg.kafka.mode.MessageModel;

import java.util.concurrent.ExecutionException;

/**
 * 回调接口方法
 * 
 * @author zuokejin
 *
 * @param <Req extends MessageModel>
 * @param <Res>
 */
public interface RetryCallback<Req, Res> {
	/**
	 * 发送请求时回调
	 * 
	 * @param request
	 * @return
	 * @throws ExecutionException
	 */
	public Res send(Req request) throws ExecutionException;

	/**
	 * 发送失败时回调
	 * 
	 * @param request
	 * @return
	 */
	public <T> T failed(Req request);
}