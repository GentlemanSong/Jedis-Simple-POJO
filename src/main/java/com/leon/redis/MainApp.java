package com.leon.redis;

import java.util.List;

import com.leon.redis.common.client.RedisPoolClient;
import com.leon.redis.serializer.JdkSerializationRedisSerializer;
import com.leon.redis.serializer.RedisSerializer;
import com.leon.redis.serializer.StringRedisSerializer;
import com.leon.redis.spring.RedisUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

/**
 * 
 * 本类简单调用JedisSimplePOJO
 * 
 * @author Leon.Song
 *
 */
public class MainApp {
	private static RedisSerializer<String> keyRedisSerializer;
	private static RedisSerializer<Object> valueRedisSerializer;

	static {
		// 初始化jedisPool，keyRedisSerializer, valueRedisSerializer
		keyRedisSerializer = new StringRedisSerializer();
		valueRedisSerializer = new JdkSerializationRedisSerializer();
	}

	public static void main(String[] args) {
		RedisUtil.set("key", "value");
		// 看redis部署需要调用其他Util类
		
		//事务实现
		caseTraction();
	}

	/**
	 * 事务操作Transaction,注意key，value一定要序列化
	 * 
	 * 事务属于异步操作
	 * 
	 */
	private static void caseTraction() {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		try {
			Transaction tx = jedis.multi();
			for (int i = 0; i < 100; i++) {
				tx.set(keyRedisSerializer.serialize("key" + i), valueRedisSerializer.serialize(("value" + i)));
			}
			List<Object> results = tx.exec();
			System.out.println(results);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			jedis.close();
		}

	}
	
	/**
	 * 管道操作Pipelined,注意key，value一定要序列化
	 * 
	 * 管道属于异步操作
	 * 
	 */
	public static void casePipelined() {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		try {
			Pipeline pipeline = jedis.pipelined();
			for (int i = 0; i < 100000; i++) {
		        pipeline.set(keyRedisSerializer.serialize("key" + i), valueRedisSerializer.serialize(("value" + i)));
		    }
			List<Object> results = pipeline.syncAndReturnAll();
			System.out.println(results);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			jedis.close();
		}
	}

}
