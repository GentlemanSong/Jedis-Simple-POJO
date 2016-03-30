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
					try {
						//最大连接数
						config.setMaxTotal(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxTotal")));
						//最少可以空闲连接数，请谨慎设置
						config.setMaxIdle(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxIdle")));
						//最长等待超时时间，获取连接时，如果连接池为空，会尝试等待此时间ms
						config.setMaxWaitMillis(
								Integer.parseInt(PropertyReader.getProperty("redis.properties", "client.MaxWaitMillis")));
						String testOnBorrow = PropertyReader.getProperty("redis.properties", "client.TestOnBorrow");
						if("true".equals(testOnBorrow)){
							config.setTestOnBorrow(true);
						}else{
							config.setTestOnBorrow(false);
						}
						//请求超时时间ms
						int timeout = Integer.parseInt(PropertyReader.getProperty("redis.properties", "timeout"));
						// 失败重试次数maxRedirections
						int maxRedirections = Integer.parseInt(PropertyReader.getProperty("redis.properties", "maxRedirections"));
						// redis集群暂未支持密码
						String shardedHostPortStrs = PropertyReader.getProperty("redis.properties", "redis-cluster-hosts");
						Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
						String[] hostPortArray = shardedHostPortStrs.split(";");
						for (int i = 0; i < hostPortArray.length; i++) {
							String[] hostPortValue = hostPortArray[i].split(":");
							jedisClusterNodes.add(new HostAndPort(hostPortValue[0], Integer.parseInt(hostPortValue[1])));
						}
						jedisCluster = new JedisCluster(jedisClusterNodes, timeout, maxRedirections, config);
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
