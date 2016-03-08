package com.leon.redis.common.client;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 
 * RedisPool客户端，用于初始化客户端， Redis单例
 * 
 * 配置参数可以提取到properties文件中
 * 
 * @author Leon.Song
 *
 */
public class RedisPoolClient {
	private static JedisPool jedisPool;
	private static Boolean isInitRedis = Boolean.FALSE;

	public static JedisPool getJedisPoolInstance() {
		if (jedisPool == null) {
			synchronized (isInitRedis) {
				if (isInitRedis.equals(Boolean.FALSE)) {
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxTotal(20);
					config.setMaxIdle(5);
					config.setMaxWaitMillis(2000);
					config.setTestOnBorrow(false);
					int timeout = 2000;
					int port = 6379;
					String usePassword = "Y";
					if ("Y".equals(usePassword)) {
						jedisPool = new JedisPool(config, "192.168.1.2", port, timeout, "password");
					} else {
						jedisPool = new JedisPool(config, "192.168.1.2", port, timeout);
					}
					isInitRedis = Boolean.TRUE;
				}
			}
		}
		return jedisPool;

	}

	/**
	 * 重置redis连接池对象
	 */
	public static void reInit() {
		jedisPool = null;
		isInitRedis = Boolean.FALSE;
	}
}
