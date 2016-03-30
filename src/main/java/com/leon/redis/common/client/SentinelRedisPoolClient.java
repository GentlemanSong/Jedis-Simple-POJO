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
					try {
						config.setMaxTotal(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxTotal")));
						config.setMaxIdle(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxIdle")));
						config.setMaxWaitMillis(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxWaitMillis")));
						String testOnBorrow = PropertyReader.getProperty("redis.properties", "client.TestOnBorrow");
						if("true".equals(testOnBorrow)){
							config.setTestOnBorrow(true);
						}else{
							config.setTestOnBorrow(false);
						}
						int timeout = Integer.parseInt(PropertyReader.getProperty("redis.properties", "timeout"));
						String masterName = PropertyReader.getProperty("redis.properties", "mastername");
						String usePassword = PropertyReader.getProperty("redis.properties", "usePassword");
						String passWord = PropertyReader.getProperty("redis.properties", "password");
						String hostPort = PropertyReader.getProperty("redis.properties", "redis-sentinel-hosts");
						String[] hostAndPort = hostPort.split(";");
						Set<String> sentinels = new HashSet<String>();
						for(int i = 0;i < hostAndPort.length;i++){
							sentinels.add(hostAndPort[i]);
						}
						if ("Y".equals(usePassword)) {
							jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, config, timeout, passWord);
						} else {
							jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, config, timeout);
						}
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
