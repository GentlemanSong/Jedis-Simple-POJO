package com.leon.redis.serializer;
/**
 * 
 *  @author Leon.Song
 *
 * @param <T>
 */
public interface RedisSerializer<T> {

	/**
	 * 序列化方法
	 */
	byte[] serialize(T t) throws Exception;

	/**
	 * 反序列化方法
	 */
	T deserialize(byte[] bytes) throws Exception;
}
