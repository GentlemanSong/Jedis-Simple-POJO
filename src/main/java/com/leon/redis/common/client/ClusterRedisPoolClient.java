package com.leon.redis.common.client;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * ClusterRedis客户端，用于初始化客户端， Redis集群
 * 
 * 配置参数可以提取到properties文件中
 * 
 * @author Leon.Song
 *
 */
public class ClusterRedisPoolClient {
	private static JedisCluster jedisCluster;
	private static Boolean isInitRedis = Boolean.FALSE;

	public static JedisCluster getJedisClusterInstance() {
		if (jedisCluster == null) {
			synchronized (isInitRedis) {
				if (isInitRedis.equals(Boolean.FALSE)) {
					JedisPoolConfig config = new JedisPoolConfig();
					config.setMaxTotal(20);
					config.setMaxIdle(5);
					config.setMaxWaitMillis(2000);
					config.setTestOnBorrow(false);
					// redis集群暂未支持密码

					String shardedHostPortStrs = "192.168.1.2:6379;192.168.1.3:6379;192.168.1.4:6379";
					Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
					String[] hostPortArray = shardedHostPortStrs.split(";");
					for (int i = 0; i < hostPortArray.length; i++) {
						String[] hostPortValue = hostPortArray[i].split(":");
						jedisClusterNodes.add(new HostAndPort(hostPortValue[0], Integer.parseInt(hostPortValue[1])));
					}
					// 失败重试次数maxRedirections
					jedisCluster = new JedisCluster(jedisClusterNodes, 2000, 3, config);
					isInitRedis = Boolean.TRUE;
				}
			}
		}
		return jedisCluster;
	}

	/**
	 * 重置redis连接池对象
	 */
	public static void reInit() {
		jedisCluster = null;
		isInitRedis = Boolean.FALSE;
	}
}
