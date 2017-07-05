package com.leon;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.leon.redis.common.RedisUtil;

public class RedisUtilTest {

	@Test
	public void testSet() {
		//向redis中set key及对应value  对已存在的可以进行value覆盖
		String set = RedisUtil.set("test", "test");
		System.out.println(set);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testSetex() {
		//set数据存在时间，单位为seconds 对已存在的可以进行value覆盖
		String result = RedisUtil.setex("testSetex",60,"setex");
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testGet() {
		//通过key获取对应value
		Object object = RedisUtil.get("test");
		System.out.println(object);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testGetNumber() throws Exception{
		//对于自增或自减key的value的获取
		//通过key获取对应value
		Object object = RedisUtil.getIncrAndDecrValue("testCr");
		System.out.println(object);
		fail("Not yet implemented");
		//测试通过
	}
	
	@Test
	public void testExists() {
		//检查key是否存在
		boolean flag = RedisUtil.exists("test");
		boolean flag1 = RedisUtil.exists("test0");
		System.out.println(flag);
		System.out.println(flag1);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testType() {
		//获取目标key存储的数据类型 
		//返回值
		//none (key不存在)
		//string (字符串)
		//list (列表)
		//set (集合)
		//zset (有序集)
		//hash (哈希表)
		String result = RedisUtil.type("test");
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testExpire() {
		//为给定key设置生存时间，如果当前key有生存时间，则更新。成功返回1，key不存在或者redis版本不支持则返回0。
		Long result = RedisUtil.expire("test2", 60);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testExpireAt() {
		RedisUtil.set("testExpireAt", "shijian");
		//为给定key设置生存时间，设置的时间以unix时间戳为参数
		//当设置成功时返回1，key不存在或者无法设置时返回0
		Long expireTime = (System.currentTimeMillis()/1000L)+60;
		System.out.println(expireTime);
		Long result = RedisUtil.expireAt("testExpireAt", expireTime);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testTtl() {
		//获取给定key的剩余生存时间
		//如果给定key不存在返回-2，给定key存在但没有设置生存时间返回-1，有key有时间按秒返回时间
		Long result = RedisUtil.ttl("testExpireAt");
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testSetbit() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetbit() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetSet() {
		fail("Not yet implemented");
	}

	@Test
	public void testDecrBy() {
		//返回自减后的值
		Long result = RedisUtil.decrBy("testCr", 2);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testDecr() {
		//返回自减后的值
		Long result = RedisUtil.decr("testCr");
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testIncrBy() {
		//返回值为自增后的key的值
		Long result = RedisUtil.incrBy("testIncr",5);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testIncr() {
		//返回值为自增后的key的值
		Long result = RedisUtil.incr("testCr1");
		System.out.println("incr-testCr:"+result);
		
		result = Long.parseLong(String.valueOf(RedisUtil.getIncrAndDecrValue("testCr1")));
		System.out.println("incr-getIncrAndDecrValue:"+result);
		
		String r1 = RedisUtil.setIncrAndDecrValue("IncrAndDecrValue", 100L);
		System.out.println("incr-getIncrAndDecrValue-r1:"+r1);
		RedisUtil.incr("IncrAndDecrValue");
		RedisUtil.incr("IncrAndDecrValue");
		RedisUtil.decr("IncrAndDecrValue");
		Long r2 = Long.parseLong(String.valueOf(RedisUtil.getIncrAndDecrValue("IncrAndDecrValue")));
		System.out.println("incr-getIncrAndDecrValue-r2:"+r2);
		
	}

	@Test
	public void testAppend() {
		fail("Not yet implemented");
	}

	@Test
	public void testSubstr() {
		fail("Not yet implemented");
	}

	@Test
	public void testHset() {
		//HSET key field value将哈希表 key 中的域 field 的值设为 value
		//创建新域且值设置成功，返回1，覆盖旧值且成功返回0
		Long result = RedisUtil.hset("hash", "jfen", "jf.10086.com");
		System.out.println(result);		
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testHget() {
		//HGET key field返回哈希表 key 中给定域 field 的值。有值返回值，没值返回null
		Object object = RedisUtil.hget("hash", "jifen");
		System.out.println(object);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testHsetnx() {
		fail("Not yet implemented");
	}

	@Test
	public void testHmset() {
		//HMSET key field value [field value ...] 同时将多个 field-value (域-值)对设置到哈希表 key 中。此命令会覆盖哈希表中已存在的域。
		//命令执行成功返回ok 当 key 不是哈希表(hash)类型时，返回一个错误
		Map<String,Object> map = new HashMap<String,Object>();
		map.put("baidu", "www.baidu.com");
		map.put("google", "www.google.com");
		map.put("asiainfo", "www.asiainfo.com.cn");
		Object result = RedisUtil.hmset("hash", map);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testHmget() {
		//HMGET key field [field ...] 返回哈希表 key 中，一个或多个给定域的值。如果给定的域不存在于哈希表，那么返回null。
		//返回值：一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样。
		List<Object> list = RedisUtil.hmget("hash", "jifen","baidu","google","yaohu");
		for (Object object : list) {
			System.out.println(object);
		}
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testHincrBy() {
		fail("Not yet implemented");
	}

	@Test
	public void testHexists() {
		fail("Not yet implemented");
	}

	@Test
	public void testDel() {
		Long result = RedisUtil.del("name");
		System.out.println(result);
		fail("Not yet implemented");
	}

	@Test
	public void testHdel() {
		//HDEL key field [field ...] 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
		//返回被成功移除的域，不包括忽略的域
		Long result = RedisUtil.hdel("hash", "baidu");
		System.out.println(result);
		fail("Not yet implemented");
	}

	@Test
	public void testHlen() {
		fail("Not yet implemented");
	}

	@Test
	public void testHkeys() {
		fail("Not yet implemented");
	}

	@Test
	public void testHvals() {
		fail("Not yet implemented");
	}

	@Test
	public void testHgetAll() {
		fail("Not yet implemented");
	}

	@Test
	public void testRpush() {
		fail("Not yet implemented");
	}

	@Test
	public void testLpush() {
		fail("Not yet implemented");
	}

	@Test
	public void testLlen() {
		fail("Not yet implemented");
	}

	@Test
	public void testLrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testLtrim() {
		fail("Not yet implemented");
	}

	@Test
	public void testLindex() {
		fail("Not yet implemented");
	}

	@Test
	public void testLset() {
		fail("Not yet implemented");
	}

	@Test
	public void testLrem() {
		fail("Not yet implemented");
	}

	@Test
	public void testLpop() {
		fail("Not yet implemented");
	}

	@Test
	public void testRpop() {
		fail("Not yet implemented");
	}

	@Test
	public void testSadd() {
		//返回值被添加的元素数量
		Object value = "\"zhangsan\" \"lisi\" \"wangwu\" \"zhaoliu\" \"who\"";
		System.out.println(value);
		Long result = RedisUtil.sadd("name", value);
		System.out.println(result);
		RedisUtil.sadd("name", "zhangsan");
		RedisUtil.sadd("name", "lisi");
		RedisUtil.sadd("name", "wangwu");
		RedisUtil.sadd("name", "zhaoliu");
		RedisUtil.sadd("name", "who");		
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testSaddBatch(){
		//批量添加元素
		Long result = RedisUtil.saddBatch("testBatch", "zhangsan","lisi","wangwu","zhaoliu","huangqi");
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}
	
	@Test
	public void testSmembers() {
		//获取集合全部元素
		Set<Object> set = RedisUtil.smembers("testBatch");
		System.out.println(set);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testSrem() {
		//删除集合中的元素，不存在的元素会被忽略
		Object value = "\"zhangsan\" \"lisi\" \"wangwu\" \"zhaoliu\" \"who\"";
		Long result = RedisUtil.srem("name", value);
		System.out.println(result);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testSpop() {
		//移除集合中的随机元素,返回随机移除的元素
		Object object = RedisUtil.spop("name");
		System.out.println(object);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testScard() {
		fail("Not yet implemented");
	}

	@Test
	public void testSismember() {
		fail("Not yet implemented");
	}

	@Test
	public void testSrandmember() {
		//获取集合中的一个随机元素
		Object object = RedisUtil.srandmember("name");
		System.out.println(object);
		fail("Not yet implemented");
		//测试通过
	}

	@Test
	public void testZadd() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrem() {
		fail("Not yet implemented");
	}

	@Test
	public void testZincrby() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrank() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrank() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrange() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrangeWithScores() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrangeWithScores() {
		fail("Not yet implemented");
	}

	@Test
	public void testZcard() {
		fail("Not yet implemented");
	}

	@Test
	public void testZscore() {
		fail("Not yet implemented");
	}

	@Test
	public void testSortString() {
		fail("Not yet implemented");
	}

	@Test
	public void testSortStringSortingParams() {
		fail("Not yet implemented");
	}

	@Test
	public void testZcount() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrangeByScoreStringDoubleDouble() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrangeByScoreStringDoubleDouble() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrangeByScoreStringDoubleDoubleIntInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrangeByScoreStringDoubleDoubleIntInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrangeByScoreWithScoresStringDoubleDouble() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrangeByScoreWithScoresStringDoubleDouble() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrangeByScoreWithScoresStringDoubleDoubleIntInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testZrevrangeByScoreWithScoresStringDoubleDoubleIntInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testZremrangeByRank() {
		fail("Not yet implemented");
	}

	@Test
	public void testZremrangeByScore() {
		fail("Not yet implemented");
	}

	@Test
	public void testLinsert() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetnx() {
		fail("Not yet implemented");
	}

}
