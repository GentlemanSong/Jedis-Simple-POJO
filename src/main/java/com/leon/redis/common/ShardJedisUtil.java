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

import com.leon.redis.common.client.ShardedRedisPoolClient;
import com.leon.redis.serializer.JdkSerializationRedisSerializer;
import com.leon.redis.serializer.RedisSerializer;
import com.leon.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

/**
 * 
 * Redis分片公共类
 * 
 * 公共类方法简单封装，支持pojo存储，key为String，value为Object
 *
 * 当前方法相对较全面，后续可以按照需要新增方法支持
 * 
 * @author Leon.Song
 *
 */
public class ShardJedisUtil {
	private static Log log = LogFactory.getLog(ShardJedisUtil.class);
	private static RedisSerializer<String> keyRedisSerializer;
	private static RedisSerializer<Object> valueRedisSerializer;

	static {
		// 初始化jedisPool，keyRedisSerializer, valueRedisSerializer
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

	private static Map<byte[], byte[]> byteMapConvertFromObject(Map<String, Object> hash) throws Exception{
		Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>(hash.size());
	    for (Entry<String, Object> entry : hash.entrySet()) {
	      bhash.put(keyRedisSerializer(entry.getKey()), valueRedisSerializer(entry.getValue()));
	    }
	    return bhash;
	}
	
	private static Map<String, Object> objectMapConvertFromByte(Map<byte[], byte[]> map) throws Exception{
		Map<String, Object> newMap = new HashMap<String, Object>(map.size());
	    for (Entry<byte[], byte[]> entry : map.entrySet()) {
	    	newMap.put(keyRedisDeserializer(entry.getKey()), valueRedisDeserializer(entry.getValue()));
	    }
	    return newMap;
	}
	
	private static List<Object> objectListConvertFromByte(List<byte[]> list) throws Exception{
		List<Object> newlist = new ArrayList<Object>();
		for(byte[] record: list){
			newlist.add(valueRedisDeserializer(record));
		}
		return newlist;
	}
	
	private static Collection<Object> objectCollectionConvertFromByte(Collection<byte[]> list) throws Exception{
		List<Object> newlist = new ArrayList<Object>();
		for(byte[] record: list){
			newlist.add(valueRedisDeserializer(record));
		}
		return newlist;
	}
	
	
	private static Set<Object> objectSetConvertFromByte(Set<byte[]> set) throws Exception{
		Set<Object> result = new HashSet<Object>(set.size());
		for(byte[] record: set){
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
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.set(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public String setex(String key, int seconds, Object value) {
		String result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.setex(keyRedisSerializer(key), seconds, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
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
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.get(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Boolean exists(String key) {
		Boolean result = false;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.exists(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static String type(String key) {
		String result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.type(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
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
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.expire(keyRedisSerializer(key), seconds);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
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
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.expireAt(keyRedisSerializer(key), unixTime);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long ttl(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.ttl(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static boolean setbit(String key, long offset, boolean value) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		boolean result = false;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.setbit(keyRedisSerializer(key), offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static boolean getbit(String key, long offset) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		boolean result = false;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getbit(keyRedisSerializer(key), offset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static long setrange(String key, long offset, Object value) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		long result = 0;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.setrange(keyRedisSerializer(key), offset, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object getrange(String key, long startOffset, long endOffset) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.getrange(keyRedisSerializer(key), startOffset, endOffset));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object getSet(String key, Object value) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.getSet(keyRedisSerializer(key), valueRedisSerializer(value)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long decrBy(String key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.decrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long decr(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.decr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long incrBy(String key, long integer) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.incrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long incr(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.incr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long append(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.append(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object substr(String key, int start, int end) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.substr(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long hset(String key, String field, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hset(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object hget(String key, String field) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.hget(keyRedisSerializer(key), keyRedisSerializer(field)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long hsetnx(String key, String field, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hsetnx(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static String hmset(String key, Map<String, Object> hash) {
		String result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hmset(keyRedisSerializer(key), byteMapConvertFromObject(hash));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static List<Object> hmget(String key, String... fields) {
		List<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			byte[][] bfields = new byte[fields.length][];
		    for (int i = 0; i < bfields.length; i++) {
		      bfields[i] = keyRedisSerializer(fields[i]);
		    }
			result = objectListConvertFromByte(shardedJedis.hmget(keyRedisSerializer(key), bfields));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long hincrBy(String key, String field, long value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hincrBy(keyRedisSerializer(key), keyRedisSerializer(field), value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Boolean hexists(String key, String field) {
		Boolean result = false;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hexists(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long del(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.del(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long hdel(String key, String field) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hdel(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long hlen(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.hlen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> hkeys(String key) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.hkeys(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Collection<Object> hvals(String key) {
		Collection<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectCollectionConvertFromByte(shardedJedis.hvals(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Map<String, Object> hgetAll(String key) {
		Map<String, Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectMapConvertFromByte(shardedJedis.hgetAll(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long rpush(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.rpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long lpush(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.lpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long llen(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.llen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static List<Object> lrange(String key, long start, long end) {
		List<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(shardedJedis.lrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static String ltrim(String key, long start, long end) {
		String result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.ltrim(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object lindex(String key, long index) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.lindex(keyRedisSerializer(key), index));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static String lset(String key, long index, Object value) {
		String result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.lset(keyRedisSerializer(key), index, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long lrem(String key, long count, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.lrem(keyRedisSerializer(key), count, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object lpop(String key) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.lpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object rpop(String key) {
		Object result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.rpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long sadd(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.sadd(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> smembers(String key) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.smembers(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long srem(String key, Object value) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Long result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.srem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object spop(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.spop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long scard(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Long result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.scard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Boolean sismember(String key, Object value) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Boolean result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.sismember(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Object srandmember(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(shardedJedis.srandmember(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zadd(String key, double score, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zadd(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrange(String key, int start, int end) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zrem(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Double zincrby(String key, double score, Object value) {
		Double result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zincrby(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zrank(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zrevrank(String key, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrevrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrange(String key, int start, int end) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrevrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrevrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zcard(String key) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zcard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Double zscore(String key, Object value) {
		Double result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zscore(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static List<Object> sort(String key) {
		List<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(shardedJedis.sort(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static List<Object> sort(String key, SortingParams sortingParameters) {
		List<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(shardedJedis.sort(keyRedisSerializer(key), sortingParameters));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zcount(String key, double min, double max) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zcount(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrangeByScore(keyRedisSerializer(key), min, max));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrevrangeByScore(keyRedisSerializer(key), max, min));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrangeByScore(keyRedisSerializer(key), min, max, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<Object> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(shardedJedis.zrevrangeByScore(keyRedisSerializer(key), max, min, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrangeByScoreWithScores(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrangeByScoreWithScores(keyRedisSerializer(key), min, max, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zremrangeByRank(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.zremrangeByScore(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public static Long linsert(String key, LIST_POSITION where, Object pivot, Object value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.linsert(keyRedisSerializer(key), where, valueRedisSerializer(pivot), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}
	
	public static Long setnx(String key, String value) {
		Long result = null;
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.setnx(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}
	
	
	public List<Object> pipelined(ShardedJedisPipeline shardedJedisPipeline) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		List<Object> result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.pipelined().getResults();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public Jedis getShard(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Jedis result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getShard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public JedisShardInfo getShardInfo(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		JedisShardInfo result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getShardInfo(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public String getKeyTag(String key) {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		String result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getKeyTag(key);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public Collection<JedisShardInfo> getAllShardInfo() {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Collection<JedisShardInfo> result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getAllShardInfo();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}

	public Collection<Jedis> getAllShards() {
		ShardedJedis shardedJedis = ShardedRedisPoolClient.getJedisPoolInstance().getResource();
		Collection<Jedis> result = null;
		if (shardedJedis == null) {
			return result;
		}
		try {
			result = shardedJedis.getAllShards();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			shardedJedis.close();
		}
		return result;
	}
	
}
