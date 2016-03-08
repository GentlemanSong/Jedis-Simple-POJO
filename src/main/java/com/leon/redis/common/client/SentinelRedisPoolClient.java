package com.leon.redis.common.client;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

/**
 * 
 * Redis哨兵Sentinel主从客户端,用于初始化客户端, redis未分片
 * 也可以基于redis分片做哨兵主从，
 * 参照https://github.com/warmbreeze/sharded-jedis-sentinel-pool
 * 
 * 配置参数可以提取到properties文件中
 * 
 * @author Leon.Song
 *
 */
public class SentinelRedisPoolClient {
	private static JedisSentinelPool jedisSentinelPool;
	private static Boolean isInitRedis = Boolean.FALSE;

	public static JedisSentinelPool getJedisPoolInstance() {
		if (jedisSentinelPool == null) {
			synchronized (isInitRedis) {
				if (isInitRedis.equals(Boolean.FALSE)) {
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxTotal(20);
					config.setMaxIdle(5);
					config.setMaxWaitMillis(2000);
					config.setTestOnBorrow(false);
					int timeout = 2000;
					String usePassword = "Y";
					
					Set<String> sentinels = new HashSet<String>();
			        sentinels.add("192.168.1.2:26379");
			        sentinels.add("192.168.1.2:26380");
			        sentinels.add("192.168.1.2:26381");
					if ("Y".equals(usePassword)) {
						jedisSentinelPool = new JedisSentinelPool("mymaster-6379", sentinels, config, timeout, "password");
					} else {
						jedisSentinelPool = new JedisSentinelPool("mymaster-6379", sentinels, config, timeout);
					}
					isInitRedis = Boolean.TRUE;
				}
			}
		}
		return jedisSentinelPool;

	}

	/**
	 * 重置redis连接池对象
	 */
	public static void reInit() {
		jedisSentinelPool = null;
		isInitRedis = Boolean.FALSE;
	}
}
