package cn.zxw.redis.util;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Repository("redisDataSource")
public class RedisDataSource{
    @Autowired
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