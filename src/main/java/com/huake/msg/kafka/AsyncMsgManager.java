package com.huake.msg.kafka;

import com.huake.msg.kafka.utils.SpringFactoriesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AsyncMsgManager {
	private final static Logger LOGGER = LoggerFactory.getLogger(AsyncMsgManager.class);
	private static AsyncMsgManager manager = new AsyncMsgManager();

	private AsyncMsgManager() {
	}

	public static AsyncMsgManager getInstance() {
		return manager;
	}

	/**
	 * 
	 * <p>
	 * Title:pushMsgToKafka
	 * </p>
	 * <p>
	 * Description: 推消息到kafka
	 * </p>
	 * 
	 * @author zuokejin
	 * @date 2019年7月16日
	 */
	public <T> void pushMsgToKafka(List<T> msg) {
		LOGGER.debug("===============准备发送消息到kafka=============");
		if (msg != null && msg.size() > 0) {
			SendMessageCallBack sendMessageCallBack = SpringFactoriesUtils.loadFactories(SendMessageCallBack.class);
			for (T item : msg) {
				sendMessageCallBack.process(item);
			}
		}

		LOGGER.debug("===============发送消息到结束=============");
	}

	public Object pullMsgFromKafka() {
		return null;
	}
}
