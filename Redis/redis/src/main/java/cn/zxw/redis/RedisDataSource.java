package cn.zxw.redis;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisDataSource{
    private JedisPool  jedisPool;

    public Jedis getRedisClient() {
    	Logger log = Logger.getLogger(RedisDataSource.class);
        try {
            Jedis jedis = jedisPool.getResource();
            return jedis;
        } catch (Exception e) {
            log.error("getRedisClent error", e);
        }
        return null;
    }

    public void returnResource(Jedis shardedJedis) {
    	jedisPool.returnResource(shardedJedis);
    }

    public void returnResource(Jedis jedis, boolean broken) {
        if (broken) {
        	jedisPool.returnBrokenResource(jedis);
        } else {
        	jedisPool.returnResource(jedis);
        }
    }
}