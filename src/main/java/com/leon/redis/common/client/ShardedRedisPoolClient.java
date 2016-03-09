package com.leon.redis.common.client;

import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * ShardedRedisPool客户端，用于初始化客户端， redis分布式
 * 
 * 配置参数可以提取到properties文件中
 * 
 * @author Leon.Song
 *
 */
public class ShardedRedisPoolClient {
	private static ShardedJedisPool shardJedisPool;
	private static Boolean isInitRedis = Boolean.FALSE;
	// 哈希算法以实例名分片
	private static final String REDIS_INSTANCE_NAME = "instance:";

	/**
	 * 获取redis pool
	 * 
	 * @param redisType
	 * @return
	 */
	public static ShardedJedisPool getJedisPoolInstance() {
		if (shardJedisPool == null) {
			synchronized (isInitRedis) {
				if (isInitRedis.equals(Boolean.FALSE)) {
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxTotal(20);
					config.setMaxIdle(5);
					config.setMaxWaitMillis(2000);
					config.setTestOnBorrow(false);
					String usePassword = "Y";
					//可以多建立几个分片 为 数据复制 预留
					String shardedHostPortStrs = "192.168.1.2:6379;192.168.1.3:6379;192.168.1.4:6379";
					int timeout = 2000;
					List<JedisShardInfo> jdsInfoList = new ArrayList<JedisShardInfo>();
					String[] hostPortArray = shardedHostPortStrs.split(";");
					for (int i = 0; i < hostPortArray.length; i++) {
						String[] hostPortValue = hostPortArray[i].split(":");
						JedisShardInfo shardInfo = new JedisShardInfo(hostPortValue[0],
								Integer.parseInt(hostPortValue[1]), timeout, REDIS_INSTANCE_NAME + i);
						if ("Y".equals(usePassword)) {
							shardInfo.setPassword("password");
						}
						jdsInfoList.add(shardInfo);
					}
					shardJedisPool = new ShardedJedisPool(config, jdsInfoList);
					isInitRedis = Boolean.TRUE;
				}
			}
		}
		return shardJedisPool;
	}

	/**
	 * 重置redis连接池对象
	 */
	public static void reInit() {
		shardJedisPool = null;
		isInitRedis = Boolean.FALSE;
	}
}
