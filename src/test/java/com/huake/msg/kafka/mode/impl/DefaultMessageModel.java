package com.huake.msg.kafka.mode.impl;

import com.huake.msg.kafka.mode.MessageModel;
import lombok.Data;
import lombok.ToString;

import java.util.Map;

/**
 * 消息模型，可自由扩展
 * @author zuokejin
 */
@Data
@ToString
public class DefaultMessageModel implements MessageModel {
    private Map<String ,Object> headers;
    private Map<String ,Object> body;

}
