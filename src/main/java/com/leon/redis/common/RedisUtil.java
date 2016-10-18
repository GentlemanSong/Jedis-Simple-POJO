package com.leon.redis.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.leon.redis.common.client.RedisPoolClient;
import com.leon.redis.serializer.JdkSerializationRedisSerializer;
import com.leon.redis.serializer.RedisSerializer;
import com.leon.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;

/**
 * 
 * Redis单例公共类
 * 
 * 公共类方法简单封装，支持pojo存储，key为String，value为Object 
 * 
 * 当前方法相对较全面，后续可以按照需要新增方法支持
 * 
 * @author Leon.Song
 *
 */
public class RedisUtil {
	private static Log log = LogFactory.getLog(RedisUtil.class);
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
		if(null!=value){
			return valueRedisSerializer.deserialize(value);
		}else{
			return null;
		}
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
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.set(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static String setex(String key, int seconds, Object value) {
		String result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.setex(keyRedisSerializer(key), seconds, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
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
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.get(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
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
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
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
	
	public static Boolean exists(String key) {
		Boolean result = false;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.exists(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static String type(String key) {
		String result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.type(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
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
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.expire(keyRedisSerializer(key), seconds);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
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
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.expireAt(keyRedisSerializer(key), unixTime);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long ttl(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.ttl(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static boolean setbit(String key, long offset, boolean value) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		boolean result = false;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.setbit(keyRedisSerializer(key), offset, value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static boolean getbit(String key, long offset) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		boolean result = false;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.getbit(keyRedisSerializer(key), offset);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static long setrange(String key, long offset, Object value) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		long result = 0;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.setrange(keyRedisSerializer(key), offset, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object getrange(String key, long startOffset, long endOffset) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.getrange(keyRedisSerializer(key), startOffset, endOffset));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object getSet(String key, Object value) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.getSet(keyRedisSerializer(key), valueRedisSerializer(value)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long decrBy(String key, long integer) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.decrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long decr(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.decr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long incrBy(String key, long integer) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.incrBy(keyRedisSerializer(key), integer);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long incr(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.incr(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long append(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.append(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object substr(String key, int start, int end) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.substr(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long hset(String key, String field, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hset(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object hget(String key, String field) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.hget(keyRedisSerializer(key), keyRedisSerializer(field)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long hsetnx(String key, String field, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hsetnx(keyRedisSerializer(key), keyRedisSerializer(field), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static String hmset(String key, Map<String, Object> hash) {
		String result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hmset(keyRedisSerializer(key), byteMapConvertFromObject(hash));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static List<Object> hmget(String key, String... fields) {
		List<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			byte[][] bfields = new byte[fields.length][];
		    for (int i = 0; i < bfields.length; i++) {
		      bfields[i] = keyRedisSerializer(fields[i]);
		    }
			result = objectListConvertFromByte(jedis.hmget(keyRedisSerializer(key), bfields));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long hincrBy(String key, String field, long value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hincrBy(keyRedisSerializer(key), keyRedisSerializer(field), value);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Boolean hexists(String key, String field) {
		Boolean result = false;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hexists(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long del(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.del(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long hdel(String key, String field) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hdel(keyRedisSerializer(key), keyRedisSerializer(field));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long hlen(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.hlen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> hkeys(String key) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.hkeys(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static List<Object> hvals(String key) {
		List<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(jedis.hvals(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Map<String, Object> hgetAll(String key) {
		Map<String, Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectMapConvertFromByte(jedis.hgetAll(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long rpush(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.rpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long lpush(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.lpush(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long llen(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.llen(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static List<Object> lrange(String key, long start, long end) {
		List<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(jedis.lrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static String ltrim(String key, long start, long end) {
		String result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.ltrim(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object lindex(String key, long index) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.lindex(keyRedisSerializer(key), index));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static String lset(String key, long index, Object value) {
		String result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.lset(keyRedisSerializer(key), index, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long lrem(String key, long count, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.lrem(keyRedisSerializer(key), count, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object lpop(String key) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.lpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object rpop(String key) {
		Object result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.rpop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long sadd(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.sadd(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
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
	public static Long saddBatch(String key , Object... values ){
		Long result = 0L;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if(jedis == null){
			return result;
		}
		try {
			for(int i = 0;i < values.length;i++){
				jedis.sadd(keyRedisSerializer(key), valueRedisSerializer(values[i]));
				result ++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	public static Set<Object> smembers(String key) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.smembers(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long srem(String key, Object value) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Long result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.srem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object spop(String key) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.spop(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long scard(String key) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Long result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.scard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Boolean sismember(String key, Object value) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Boolean result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.sismember(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Object srandmember(String key) {
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		Object result = null;
		if (jedis == null) {
			return result;
		}
		try {
			result = valueRedisDeserializer(jedis.srandmember(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zadd(String key, double score, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zadd(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrange(String key, int start, int end) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zrem(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrem(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Double zincrby(String key, double score, Object value) {
		Double result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zincrby(keyRedisSerializer(key), score, valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zrank(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zrevrank(String key, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrevrank(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrange(String key, int start, int end) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrevrange(keyRedisSerializer(key), start, end));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeWithScores(String key, int start, int end) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrevrangeWithScores(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zcard(String key) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zcard(keyRedisSerializer(key));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Double zscore(String key, Object value) {
		Double result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zscore(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static List<Object> sort(String key) {
		List<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(jedis.sort(keyRedisSerializer(key)));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static List<Object> sort(String key, SortingParams sortingParameters) {
		List<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectListConvertFromByte(jedis.sort(keyRedisSerializer(key), sortingParameters));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zcount(String key, double min, double max) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zcount(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrangeByScore(keyRedisSerializer(key), min, max));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrevrangeByScore(keyRedisSerializer(key), max, min));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrangeByScore(String key, double min, double max, int offset, int count) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrangeByScore(keyRedisSerializer(key), min, max, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Object> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		Set<Object> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = objectSetConvertFromByte(jedis.zrevrangeByScore(keyRedisSerializer(key), max, min, offset, count));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrangeByScoreWithScores(keyRedisSerializer(key), min, max);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrangeByScoreWithScores(keyRedisSerializer(key), min, max, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		Set<Tuple> result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zrevrangeByScoreWithScores(keyRedisSerializer(key), max, min, offset, count);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zremrangeByRank(String key, int start, int end) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zremrangeByRank(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long zremrangeByScore(String key, double start, double end) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.zremrangeByScore(keyRedisSerializer(key), start, end);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}

	public static Long linsert(String key, LIST_POSITION where, Object pivot, Object value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.linsert(keyRedisSerializer(key), where, valueRedisSerializer(pivot), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}
	
	public static Long setnx(String key, String value) {
		Long result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.setnx(keyRedisSerializer(key), valueRedisSerializer(value));
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}
	
	/**
	 * 事务操作Transaction
	 * 
	 * 调用该方法外层需要序列化数据
	 * 
	 * @return
	 */
	public static Transaction multi() {
		Transaction result = null;
		Jedis jedis = RedisPoolClient.getJedisPoolInstance().getResource();
		if (jedis == null) {
			return result;
		}
		try {
			result = jedis.multi();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			jedis.close();
		}
		return result;
	}
	
}
