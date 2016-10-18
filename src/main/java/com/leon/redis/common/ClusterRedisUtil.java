package com.leon.redis.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.leon.redis.common.client.ClusterRedisPoolClient;
import com.leon.redis.serializer.JdkSerializationRedisSerializer;
import com.leon.redis.serializer.RedisSerializer;
import com.leon.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.BinaryClient.LIST_POSITION;

/**
 * 
 * Redis集群 公共类
 * 
 * 公共类方法简单封装，支持pojo存储，key为String，value为Object 
 * 
 * 当前方法相对较全面，后续可以按照需要新增方法支持
 * 
 * @author Leon.Song
 *
 */
public class ClusterRedisUtil {
	private static Log log = LogFactory.getLog(ClusterRedisUtil.class);
	//private static JedisCluster jedisCluster;
	private static RedisSerializer<String> keyRedisSerializer;
	private static RedisSerializer<Object> valueRedisSerializer;
	
	static {
		keyRedisSerializer = new StringRedisSerializer();
		valueRedisSerializer = new JdkSerializationRedisSerializer();
	}

	/**
	 * Key序列化
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	private static byte[] keyRedisSerializer(String key) throws Exception {
		return keyRedisSerializer.serialize(key);
	}

	/**
	 * Key反序列化
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	private static String keyRedisDeserializer(byte[] key) throws Exception {
		return keyRedisSerializer.deserialize(key);
	}
	
	/**
	 * value序列化
	 * 
	 * @param key
	 *            Object对象需要继承序列化接口
	 * @return
	 * @throws Exception
	 */
	private static byte[] valueRedisSerializer(Object value) throws Exception {
		return valueRedisSerializer.serialize(value);
	}

	@Test
	public void testSer(){
		valueRedisSerializer = new JdkSerializationRedisSerializer();
		Object value = 1;
		try {
			System.out.println(valueRedisSerializer.serialize(value));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * value反序列化
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	private static Object valueRedisDeserializer(byte[] value) throws Exception {
		if(null!=value){
			return valueRedisSerializer.deserialize(value);
		}else{
			return null;
		}
	}
	@Test
	public void testDes(){
		valueRedisSerializer = new JdkSerializationRedisSerializer();
		try {
			byte[] value = valueRedisSerializer.serialize(1);
			System.out.println(valueRedisSerializer.deserialize(value));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static Map<byte[], byte[]> byteMapConvertFromObject(Map<String, Object> hash) throws Exception {
		Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
		for (Entry<String, Object> entry : hash.entrySet()) {
			bhash.put(keyRedisSerializer(entry.getKey()), valueRedisSerializer(entry.getValue()));
		}
		return bhash;
	}

	private static Map<String, Object> objectMapConvertFromByte(Map<byte[], byte[]> map) throws Exception {
		Map<String, Object> newMap = new HashMap<String, Object>(map.size());
		for (Entry<byte[], byte[]> entry : map.entrySet()) {
			newMap.put(keyRedisDeserializer(entry.getKey()), valueRedisDeserializer(entry.getValue()));
		}
		return newMap;
	}
	
	private static Collection<Object> objectCollectionConvertFromByte(Collection<byte[]> list) throws Exception{
		List<Object> newlist = new ArrayList<Object>();
		for(byte[] record: list){
			newlist.add(valueRedisDeserializer(record));
		}
		return newlist;
	}

	private static List<Object> objectListConvertFromByte(List<byte[]> list) throws Exception {
		List<Object> newlist = new ArrayList<Object>();
		for (byte[] record : list) {
			newlist.add(valueRedisDeserializer(record));
		}
		return newlist;
	}

	private static Set<Object> objectSetConvertFromByte(Set<byte[]> set) throws Exception {
		Set<Object> result = new HashSet<Object>(set.size());
		for (byte[] record : set) {
			if (record == null) {
				result.add(null);
			} else {
				result.add(valueRedisDeserializer(record));
			}
		}
		return result;
	}

	/**
	 * 设置键值
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public static String set(String key, Object value) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().set(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String setex(String key, int seconds, Object value) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().setex(keyRedisSerializer(key), seconds, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	/**
	 * 获取单个值，非Redis自增自减
	 * 
	 * @param key
	 * @return
	 */
	public static Object get(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().get(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
	
	/**
	 * 获取Redis自增或自减操作的值，该类操作的值无需序列化
	 * 
	 * @param key
	 * @return
	 */
	public static long getInCrDeCrValue(String key) throws Exception{
		String result = null;
		JedisCluster jedis = ClusterRedisPoolClient.getJedisClusterInstance();
		if (jedis == null) {
			throw new Exception("No value found from Redis, please check");
		}
		try {
			result = jedis.get(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		if(result!=null){
			return Long.parseLong(result);
		} else {
			throw new Exception("No value found from Redis, please check");
		}
	}

	/**
	 * 获取自增或自减key的值
	 * 
	 * @param key
	 * @return
	 */
	public static Object getNumber(String key){
		Object result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().get(key);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public static Boolean exists(String key) {
		Boolean result = false;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().exists(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String type(String key) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().type(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	/**
	 * 在某段时间后实现
	 * 
	 * @param key
	 * @param unixTime
	 * @return
	 */
	public static Long expire(String key, int seconds) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().expire(keyRedisSerializer(key), seconds);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	/**
	 * 在某个时间点失效
	 * 
	 * @param key
	 * @param unixTime
	 * @return
	 */
	public static Long expireAt(String key, long unixTime) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().expireAt(keyRedisSerializer(key), unixTime);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long ttl(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().ttl(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static boolean setbit(String key, long offset, boolean value) {
		boolean result = false;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().setbit(keyRedisSerializer(key), offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static boolean getbit(String key, long offset) {
		boolean result = false;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().getbit(keyRedisSerializer(key), offset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static long setrange(String key, long offset, Object value) {
		long result = 0;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().setrange(keyRedisSerializer(key), offset, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object getrange(String key, long startOffset, long endOffset) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().getrange(keyRedisSerializer(key), startOffset, endOffset));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object getSet(String key, Object value) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().getSet(keyRedisSerializer(key), valueRedisSerializer(value)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long decrBy(String key, long integer) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().decrBy(keyRedisSerializer(key), integer);
			System.out.println(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long decr(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().decr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long incrBy(String key, long integer) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().incrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long incr(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().incr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long append(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().append(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object substr(String key, int start, int end) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().substr(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hset(String key, String field, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hset(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object hget(String key, String field) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().hget(keyRedisSerializer(key), keyRedisSerializer(field)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hsetnx(String key, String field, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hsetnx(keyRedisSerializer(key), keyRedisSerializer(field),
					valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String hmset(String key, Map<String, Object> hash) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hmset(keyRedisSerializer(key), byteMapConvertFromObject(hash));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> hmget(String key, String... fields) {
		List<Object> result = null;
		try {
			byte[][] bfields = new byte[fields.length][];
			for (int i = 0; i < bfields.length; i++) {
				bfields[i] = keyRedisSerializer(fields[i]);
			}
			result = objectListConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().hmget(keyRedisSerializer(key), bfields));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hincrBy(String key, String field, long value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hincrBy(keyRedisSerializer(key), keyRedisSerializer(field), value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Boolean hexists(String key, String field) {
		Boolean result = false;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hexists(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long del(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().del(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hdel(String key, String field) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hdel(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hlen(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().hlen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> hkeys(String key) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().hkeys(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Collection<Object> hvals(String key) {
		Collection<Object> result = null;
		try {
			result = objectCollectionConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().hvals(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Map<String, Object> hgetAll(String key) {
		Map<String, Object> result = null;
		try {
			result = objectMapConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().hgetAll(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long rpush(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().rpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long lpush(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().lpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long llen(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().llen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> lrange(String key, long start, long end) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().lrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String ltrim(String key, long start, long end) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().ltrim(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object lindex(String key, long index) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().lindex(keyRedisSerializer(key), index));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String lset(String key, long index, Object value) {
		String result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().lset(keyRedisSerializer(key), index, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long lrem(String key, long count, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().lrem(keyRedisSerializer(key), count, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object lpop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().lpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object rpop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().rpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long sadd(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().sadd(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
	
	/**
	 * 批量添加元素
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	public static Long saddBatch(String key, Object... values) {
		Long result = 0L;
		try {
			for(int i = 0;i < values.length;i++){
				ClusterRedisPoolClient.getJedisClusterInstance().sadd(keyRedisSerializer(key), valueRedisSerializer(values[i]));
				result ++;
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> smembers(String key) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().smembers(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long srem(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().srem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object spop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().spop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long scard(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().scard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Boolean sismember(String key, Object value) {
		Boolean result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().sismember(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object srandmember(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(ClusterRedisPoolClient.getJedisClusterInstance().srandmember(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zadd(String key, double score, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zadd(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrange(String key, int start, int end) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().zrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrem(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Double zincrby(String key, double score, Object value) {
		Double result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zincrby(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrank(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrevrank(String key, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrevrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrange(String key, int start, int end) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().zrevrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrevrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zcard(String key) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zcard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Double zscore(String key, Object value) {
		Double result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zscore(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> sort(String key) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().sort(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> sort(String key, SortingParams sortingParameters) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().sort(keyRedisSerializer(key), sortingParameters));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zcount(String key, double min, double max) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zcount(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().zrangeByScore(keyRedisSerializer(key), min, max));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(ClusterRedisPoolClient.getJedisClusterInstance().zrevrangeByScore(keyRedisSerializer(key), max, min));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(
					ClusterRedisPoolClient.getJedisClusterInstance().zrangeByScore(keyRedisSerializer(key), min, max, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(
					ClusterRedisPoolClient.getJedisClusterInstance().zrevrangeByScore(keyRedisSerializer(key), max, min, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrangeByScoreWithScores(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrangeByScoreWithScores(keyRedisSerializer(key), min, max, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zremrangeByRank(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().zremrangeByScore(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long linsert(String key, LIST_POSITION where, Object pivot, Object value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().linsert(keyRedisSerializer(key), where, valueRedisSerializer(pivot),
					valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long setnx(String key, String value) {
		Long result = null;
		try {
			result = ClusterRedisPoolClient.getJedisClusterInstance().setnx(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
}
