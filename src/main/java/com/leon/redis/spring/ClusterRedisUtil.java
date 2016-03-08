package com.leon.redis.spring;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.leon.redis.serializer.RedisSerializer;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.JedisCluster;

/**
 * 
 * Redis公共类  Redis集群
 * 
 * 公共类方法简单封装，支持pojo存储，key为String，value为Object
 * 
 * 当前方法相对较全面，后续可以按照需要新增方法支持
 * 
 * @author Leon.Song
 *
 */
@Component
public class ClusterRedisUtil {
	private static Log log = LogFactory.getLog(ClusterRedisUtil.class);
	@Autowired
	private static JedisCluster jedisCluster;
	@Autowired
	private static RedisSerializer<String> keyRedisSerializer;
	@Autowired
	private static RedisSerializer<Object> valueRedisSerializer;

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

	/**
	 * value反序列化
	 * 
	 * @param key
	 * @return
	 * @throws Exception
	 */
	private static Object valueRedisDeserializer(byte[] value) throws Exception {
		return valueRedisSerializer.deserialize(value);
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
			result = jedisCluster.set(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public String setex(String key, int seconds, Object value) {
		String result = null;
		try {
			result = jedisCluster.setex(keyRedisSerializer(key), seconds, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	/**
	 * 获取单个值
	 * 
	 * @param key
	 * @return
	 */
	public static Object get(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.get(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Boolean exists(String key) {
		Boolean result = false;
		try {
			result = jedisCluster.exists(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String type(String key) {
		String result = null;
		try {
			result = jedisCluster.type(keyRedisSerializer(key));
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
			result = jedisCluster.expire(keyRedisSerializer(key), seconds);
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
			result = jedisCluster.expireAt(keyRedisSerializer(key), unixTime);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long ttl(String key) {
		Long result = null;
		try {
			result = jedisCluster.ttl(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static boolean setbit(String key, long offset, boolean value) {
		boolean result = false;
		try {
			result = jedisCluster.setbit(keyRedisSerializer(key), offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static boolean getbit(String key, long offset) {
		boolean result = false;
		try {
			result = jedisCluster.getbit(keyRedisSerializer(key), offset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static long setrange(String key, long offset, Object value) {
		long result = 0;
		try {
			result = jedisCluster.setrange(keyRedisSerializer(key), offset, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object getrange(String key, long startOffset, long endOffset) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.getrange(keyRedisSerializer(key), startOffset, endOffset));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object getSet(String key, Object value) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.getSet(keyRedisSerializer(key), valueRedisSerializer(value)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long decrBy(String key, long integer) {
		Long result = null;
		try {
			result = jedisCluster.decrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long decr(String key) {
		Long result = null;
		try {
			result = jedisCluster.decr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long incrBy(String key, long integer) {
		Long result = null;
		try {
			result = jedisCluster.incrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long incr(String key) {
		Long result = null;
		try {
			result = jedisCluster.incr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long append(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.append(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object substr(String key, int start, int end) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.substr(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hset(String key, String field, Object value) {
		Long result = null;
		try {
			result = jedisCluster.hset(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object hget(String key, String field) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.hget(keyRedisSerializer(key), keyRedisSerializer(field)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hsetnx(String key, String field, Object value) {
		Long result = null;
		try {
			result = jedisCluster.hsetnx(keyRedisSerializer(key), keyRedisSerializer(field),
					valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String hmset(String key, Map<String, Object> hash) {
		String result = null;
		try {
			result = jedisCluster.hmset(keyRedisSerializer(key), byteMapConvertFromObject(hash));
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
			result = objectListConvertFromByte(jedisCluster.hmget(keyRedisSerializer(key), bfields));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hincrBy(String key, String field, long value) {
		Long result = null;
		try {
			result = jedisCluster.hincrBy(keyRedisSerializer(key), keyRedisSerializer(field), value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Boolean hexists(String key, String field) {
		Boolean result = false;
		try {
			result = jedisCluster.hexists(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long del(String key) {
		Long result = null;
		try {
			result = jedisCluster.del(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hdel(String key, String field) {
		Long result = null;
		try {
			result = jedisCluster.hdel(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long hlen(String key) {
		Long result = null;
		try {
			result = jedisCluster.hlen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> hkeys(String key) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.hkeys(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Collection<Object> hvals(String key) {
		Collection<Object> result = null;
		try {
			result = objectCollectionConvertFromByte(jedisCluster.hvals(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Map<String, Object> hgetAll(String key) {
		Map<String, Object> result = null;
		try {
			result = objectMapConvertFromByte(jedisCluster.hgetAll(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long rpush(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.rpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long lpush(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.lpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long llen(String key) {
		Long result = null;
		try {
			result = jedisCluster.llen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> lrange(String key, long start, long end) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(jedisCluster.lrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String ltrim(String key, long start, long end) {
		String result = null;
		try {
			result = jedisCluster.ltrim(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object lindex(String key, long index) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.lindex(keyRedisSerializer(key), index));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static String lset(String key, long index, Object value) {
		String result = null;
		try {
			result = jedisCluster.lset(keyRedisSerializer(key), index, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long lrem(String key, long count, Object value) {
		Long result = null;
		try {
			result = jedisCluster.lrem(keyRedisSerializer(key), count, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object lpop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.lpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object rpop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.rpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long sadd(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.sadd(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> smembers(String key) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.smembers(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long srem(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.srem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object spop(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.spop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long scard(String key) {
		Long result = null;
		try {
			result = jedisCluster.scard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Boolean sismember(String key, Object value) {
		Boolean result = null;
		try {
			result = jedisCluster.sismember(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Object srandmember(String key) {
		Object result = null;
		try {
			result = valueRedisDeserializer(jedisCluster.srandmember(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zadd(String key, double score, Object value) {
		Long result = null;
		try {
			result = jedisCluster.zadd(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrange(String key, int start, int end) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.zrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrem(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.zrem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Double zincrby(String key, double score, Object value) {
		Double result = null;
		try {
			result = jedisCluster.zincrby(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrank(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.zrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zrevrank(String key, Object value) {
		Long result = null;
		try {
			result = jedisCluster.zrevrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrange(String key, int start, int end) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.zrevrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrevrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zcard(String key) {
		Long result = null;
		try {
			result = jedisCluster.zcard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Double zscore(String key, Object value) {
		Double result = null;
		try {
			result = jedisCluster.zscore(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> sort(String key) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(jedisCluster.sort(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static List<Object> sort(String key, SortingParams sortingParameters) {
		List<Object> result = null;
		try {
			result = objectListConvertFromByte(jedisCluster.sort(keyRedisSerializer(key), sortingParameters));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zcount(String key, double min, double max) {
		Long result = null;
		try {
			result = jedisCluster.zcount(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.zrangeByScore(keyRedisSerializer(key), min, max));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(jedisCluster.zrevrangeByScore(keyRedisSerializer(key), max, min));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(
					jedisCluster.zrangeByScore(keyRedisSerializer(key), min, max, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<Object> result = null;
		try {
			result = objectSetConvertFromByte(
					jedisCluster.zrevrangeByScore(keyRedisSerializer(key), max, min, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrangeByScoreWithScores(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrangeByScoreWithScores(keyRedisSerializer(key), min, max, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		try {
			result = jedisCluster.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		try {
			result = jedisCluster.zremrangeByRank(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		try {
			result = jedisCluster.zremrangeByScore(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long linsert(String key, LIST_POSITION where, Object pivot, Object value) {
		Long result = null;
		try {
			result = jedisCluster.linsert(keyRedisSerializer(key), where, valueRedisSerializer(pivot),
					valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}

	public static Long setnx(String key, String value) {
		Long result = null;
		try {
			result = jedisCluster.setnx(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return result;
	}
}
