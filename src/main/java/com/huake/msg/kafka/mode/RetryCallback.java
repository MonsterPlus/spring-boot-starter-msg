package com.huake.msg.kafka.mode;

import java.util.concurrent.ExecutionException;

/**
 * 回调接口方法
 * 
 * @author zuokejin
 *
 * @param <Req>
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
	 * @param <T>
	 * @param request
	 * @return
	 */
	public <T> T faild(Req request);
}