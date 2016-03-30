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
						String usePassword = PropertyReader.getProperty("redis.properties", "usePassword");
						String passWord = PropertyReader.getProperty("redis.properties", "password");
						String hostPort = PropertyReader.getProperty("redis.properties", "redis-host");
						System.out.println(hostPort);
						String[] hostAndPort = hostPort.split(":");
						if ("Y".equals(usePassword)) {
							jedisPool = new JedisPool(config, hostAndPort[0], Integer.parseInt(hostAndPort[1]), timeout, passWord);
						} else {
							jedisPool = new JedisPool(config, hostAndPort[0], Integer.parseInt(hostAndPort[1]), timeout);
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
