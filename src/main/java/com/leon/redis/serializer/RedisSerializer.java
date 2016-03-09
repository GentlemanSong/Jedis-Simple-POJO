package com.leon.redis.serializer;
/**
 * 
 * 把序列化单独定义为普通类而非公共静态类是为了兼容任何序列化实现
 * 
 * @author Leon.Song
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
