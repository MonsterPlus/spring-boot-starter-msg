package com.huake.msg.kafka;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Message implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7890137859966250937L;

	private Map<String, Object> headers;
	private Map<String, Object> body;
}
