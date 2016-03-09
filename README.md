Jedis Simple POJO -- by Leon.Song

Jedis是redis的java客户端，提供了基础的Redis Java API。jedis相较于spring-data-redis(过渡封装不支持集群等)更灵活。
Jedis针对redis的各种部署架构提供API, 支持redis单例、redis主从Sentinel哨兵、redis shard分布式、redis cluster集群等。如果要建立redis的高可用性（HA）环境，一种方案是基于 分片主从的 sentinel，一种方案是使用cluster。描述如下：
Traditional Redis - Master and Slaves monitored by a sentinel.
Redis Cluster - In alpha mode now, where multiple masters (with their slaves) hold their respective shards of the data. 

Jedis仅支持String和byte[]操作，在日常的项目集成中具有局限性，现基于jedis进行简单的POJO封装，key为String, value为Object.所有value都需要继承序列化接口。Jedis支持的String操作底层依然是基于byte[],只是做了序列化，现把序列化方法提出为JdkSerializationRedisSerializer.java 基于JDK原生ObjectOutputStream和ObjectInputStream进行序列化，效率高。
StringRedisSerializer.java 字符串的场景，根据指定的charset对数据的字节序列编码成string，是“new String(bytes, charset)”和“string.getBytes(charset)”的直接封装。是最轻量级和高效的策略。
注： 当前策略同spring-data-redis序列化策略一致。

当前工程包含 redis集群公共类、redis单例公共类、redis sentinel公共类、redis分片公共类
com.leon.redis.common.ClusterRedisUtil.java
com.leon.redis.common.RedisUtil.java 
com.leon.redis.common.SentinelRedisUtil.java 
com.leon.redis.common.ShardJedisUtil.java

spring注入方式
配置文件加载类：com.leon.redis.spring下redis-context.xml
com.leon.redis.spring.ClusterRedisUtil.java
com.leon.redis.spring.RedisUtil.java 
com.leon.redis.spring.SentinelRedisUtil.java 
com.leon.redis.spring.ShardJedisUtil.java

可根据需要使用相关的工程类，分享，沟通，重构，优化。当前项目不支持事务，调用侵入性强，持续测试优化中....

入口类MainApp.java

关键字： Jedis, POJO, 连接池, 单例, 分布式, 集群, 事务, 管道







