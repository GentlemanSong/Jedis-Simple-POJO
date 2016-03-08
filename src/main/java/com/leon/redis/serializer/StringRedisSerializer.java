package com.leon.redis.serializer;

import java.nio.charset.Charset;

/**
 * 序列化实现
 * 
 * 根据指定的charset对数据的字节序列编码成string，默认charset为"UTF8"，
 * 是new String(bytes, charset)和string.getBytes(charset)的直接封装。
 * 
 * 类Spring Data Redis实现
 * 
 * @author Leon.Song
 *
 */
public class StringRedisSerializer implements RedisSerializer<String> {

	private final Charset charset;

	public StringRedisSerializer() {
		this(Charset.forName("UTF8"));
	}

	public StringRedisSerializer(Charset charset) {
		this.charset = charset;
	}

	
	public String deserialize(byte[] bytes) {
		return (bytes == null ? null : new String(bytes, charset));
	}

	
	public byte[] serialize(String string) {
		return (string == null ? null : string.getBytes(charset));
	}

}
