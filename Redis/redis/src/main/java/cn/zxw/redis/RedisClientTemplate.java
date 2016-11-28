package cn.zxw.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

public class RedisClientTemplate {
	
    private static final Logger log =Logger.getLogger(RedisClientTemplate.class);
    public enum RedisDB {  
    	//database index 
        DEV_KEY(0), DEV_CONTENT(1);  
        private int index;  
        private RedisDB(int index) {  
            this.index = index;  
        }  
		public int getIndex() {
			return index;
		}
		public void setIndex(int index) {
			this.index = index;
		}  
        
    }  
    private RedisDataSource redisDataSource;
    /**
     *  disconnect jedis
     */
    public void disconnect() {
        Jedis jedis = redisDataSource.getRedisClient();
        jedis.disconnect();
    }
    
    /**
     * set single String value into redis
     * 
     * @param key string key
     * @param value String value
     * @return
     */
    public String set(String key, String value,int dbIndex) {
        String result = null;
        
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
       
        boolean broken = false;
        try {
            result = jedis.set(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    /**
     * get single String value
     * 
     * @param key the String key
     * @return
     */
    public String get(String key,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.get(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }
    /**
     * whether has the key
     * @param key
     * @return
     */
    public Boolean exists(String key,int dbIndex) {
        Boolean result = false;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.exists(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String type(String key,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.type(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
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
    public Long expire(String key, int seconds,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.expire(key, seconds);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
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
    public Long expireAt(String key, long unixTime,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.expireAt(key, unixTime);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long ttl(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.ttl(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public boolean setbit(String key, long offset, boolean value,int dbIndex) {

    	Jedis jedis = redisDataSource.getRedisClient();
        boolean result = false;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.setbit(key, offset, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public boolean getbit(String key, long offset,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        boolean result = false;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;

        try {
            result = jedis.getbit(key, offset);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public long setrange(String key, long offset, String value,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        long result = 0;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.setrange(key, offset, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String getrange(String key, long startOffset, long endOffset,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        String result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.getrange(key, startOffset, endOffset);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String getSet(String key, String value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.getSet(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long setnx(String key, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.setnx(key, value);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String setex(String key, int seconds, String value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.setex(key, seconds, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long decrBy(String key, long integer,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.decrBy(key, integer);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long decr(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.decr(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long incrBy(String key, long integer,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.incrBy(key, integer);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long incr(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.incr(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long append(String key, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.append(key, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String substr(String key, int start, int end,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.substr(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hset(String key, String field, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hset(key, field, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String hget(String key, String field,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hget(key, field);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hsetnx(String key, String field, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hsetnx(key, field, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String hmset(String key, Map<String, String> hash,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hmset(key, hash);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<String> hmget(String key,int dbIndex, String... fields) {
        List<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hmget(key, fields);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hincrBy(String key, String field, long value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hincrBy(key, field, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Boolean hexists(String key, String field,int dbIndex) {
        Boolean result = false;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hexists(key, field);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long del(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.del(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hdel(String key, String field,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hdel(key, field);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hlen(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hlen(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> hkeys(String key,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hkeys(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<String> hvals(String key,int dbIndex) {
        List<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hvals(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Map<String, String> hgetAll(String key,int dbIndex) {
        Map<String, String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hgetAll(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    // ================list ====== l表示 list或 left, r表示right====================
    public Long rpush(String key, String string,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.rpush(key, string);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long lpush(String key, String string,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lpush(key, string);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long llen(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.llen(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<String> lrange(String key, long start, long end,int dbIndex) {
        List<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lrange(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String ltrim(String key, long start, long end,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.ltrim(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String lindex(String key, long index,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lindex(key, index);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String lset(String key, long index, String value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lset(key, index, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long lrem(String key, long count, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lrem(key, count, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String lpop(String key,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.lpop(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String rpop(String key,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.rpop(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    //return 1 add a not exist value ,
    //return 0 add a exist value
    public Long sadd(String key, String member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.sadd(key, member);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> smembers(String key,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.smembers(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long srem(String key, String member,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();

        Long result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.srem(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String spop(String key,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        String result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.spop(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long scard(String key,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        Long result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.scard(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Boolean sismember(String key, String member,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        Boolean result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.sismember(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String srandmember(String key,int dbIndex) {
    	Jedis jedis = redisDataSource.getRedisClient();
        String result = null;
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.srandmember(key);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zadd(String key, double score, String member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.zadd(key, score, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrange(String key, int start, int end,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.zrange(key, start, end);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrem(String key, String member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.zrem(key, member);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Double zincrby(String key, double score, String member,int dbIndex) {
        Double result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zincrby(key, score, member);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrank(String key, String member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrank(key, member);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrevrank(String key, String member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrank(key, member);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrevrange(String key, int start, int end,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrange(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeWithScores(String key, int start, int end,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrangeWithScores(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeWithScores(String key, int start, int end,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrangeWithScores(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zcard(String key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zcard(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Double zscore(String key, String member,int dbIndex) {
        Double result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zscore(key, member);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<String> sort(String key,int dbIndex) {
        List<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.sort(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<String> sort(String key, SortingParams sortingParameters,int dbIndex) {
        List<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.sort(key, sortingParameters);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zcount(String key, double min, double max,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zcount(key, min, max);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrangeByScore(key, min, max);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScore(key, max, min);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrangeByScore(key, min, max, offset, count);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count,int dbIndex) {
        Set<String> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScore(key, max, min, offset, count);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrangeByScoreWithScores(key, min, max);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScoreWithScores(key, max, min);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrangeByScoreWithScores(key, min, max, offset, count);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zremrangeByRank(String key, int start, int end,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zremrangeByRank(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zremrangeByScore(String key, double start, double end,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zremrangeByScore(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long linsert(String key, LIST_POSITION where, String pivot, String value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.linsert(key, where, pivot, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String set(byte[] key, byte[] value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.set(key, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] get(byte[] key,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.get(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Boolean exists(byte[] key,int dbIndex) {
        Boolean result = false;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.exists(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String type(byte[] key,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.type(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long expire(byte[] key, int seconds,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.expire(key, seconds);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long expireAt(byte[] key, long unixTime,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.expireAt(key, unixTime);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long ttl(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.ttl(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] getSet(byte[] key, byte[] value,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.getSet(key, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long setnx(byte[] key, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.setnx(key, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String setex(byte[] key, int seconds, byte[] value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.setex(key, seconds, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long decrBy(byte[] key, long integer,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.decrBy(key, integer);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long decr(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.decr(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long incrBy(byte[] key, long integer,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.incrBy(key, integer);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long incr(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.incr(key);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long append(byte[] key, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.append(key, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] substr(byte[] key, int start, int end,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.substr(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hset(byte[] key, byte[] field, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hset(key, field, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] hget(byte[] key, byte[] field,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hget(key, field);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hsetnx(key, field, value);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hmset(key, hash);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<byte[]> hmget(byte[] key,int dbIndex,byte[]... fields) {
        List<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hmget(key, fields);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hincrBy(byte[] key, byte[] field, long value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hincrBy(key, field, value);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Boolean hexists(byte[] key, byte[] field,int dbIndex) {
        Boolean result = false;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hexists(key, field);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hdel(byte[] key, byte[] field,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hdel(key, field);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long hlen(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hlen(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> hkeys(byte[] key,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hkeys(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Collection<byte[]> hvals(byte[] key,int dbIndex) {
        Collection<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hvals(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Map<byte[], byte[]> hgetAll(byte[] key,int dbIndex) {
        Map<byte[], byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.hgetAll(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long rpush(byte[] key, byte[] string,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.rpush(key, string);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long lpush(byte[] key, byte[] string,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lpush(key, string);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long llen(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.llen(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<byte[]> lrange(byte[] key, int start, int end,int dbIndex) {
        List<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lrange(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String ltrim(byte[] key, int start, int end,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.ltrim(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] lindex(byte[] key, int index,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lindex(key, index);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public String lset(byte[] key, int index, byte[] value,int dbIndex) {
        String result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lset(key, index, value);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long lrem(byte[] key, int count, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lrem(key, count, value);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] lpop(byte[] key,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.lpop(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] rpop(byte[] key,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.rpop(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long sadd(byte[] key, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.sadd(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> smembers(byte[] key,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.smembers(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long srem(byte[] key, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.srem(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] spop(byte[] key,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.spop(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long scard(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.scard(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Boolean sismember(byte[] key, byte[] member,int dbIndex) {
        Boolean result = false;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.sismember(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public byte[] srandmember(byte[] key,int dbIndex) {
        byte[] result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.srandmember(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zadd(byte[] key, double score, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zadd(key, score, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrange(byte[] key, int start, int end,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrange(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrem(byte[] key, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrem(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Double zincrby(byte[] key, double score, byte[] member,int dbIndex) {
        Double result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zincrby(key, score, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrank(byte[] key, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrank(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zrevrank(byte[] key, byte[] member,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrank(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrevrange(byte[] key, int start, int end,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrange(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeWithScores(byte[] key, int start, int end,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrangeWithScores(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrangeWithScores(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zcard(byte[] key,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zcard(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Double zscore(byte[] key, byte[] member,int dbIndex) {
        Double result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zscore(key, member);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<byte[]> sort(byte[] key,int dbIndex) {
        List<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.sort(key);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public List<byte[]> sort(byte[] key, SortingParams sortingParameters,int dbIndex) {
        List<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.sort(key, sortingParameters);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zcount(byte[] key, double min, double max,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zcount(key, min, max);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrangeByScore(key, min, max);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrangeByScore(key, min, max, offset, count);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrangeByScoreWithScores(key, min, max);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrangeByScoreWithScores(key, min, max, offset, count);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScore(key, max, min);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count,int dbIndex) {
        Set<byte[]> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScore(key, max, min, offset, count);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScoreWithScores(key, max, min);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count,int dbIndex) {
        Set<Tuple> result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zrevrangeByScoreWithScores(key, max, min, offset, count);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zremrangeByRank(byte[] key, int start, int end,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zremrangeByRank(key, start, end);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long zremrangeByScore(byte[] key, double start, double end,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.zremrangeByScore(key, start, end);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }

    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value,int dbIndex) {
        Long result = null;
        Jedis jedis = redisDataSource.getRedisClient();
        if (jedis == null) {
            return result;
        }
        boolean broken = false;
        try {

            result = jedis.linsert(key, where, pivot, value);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            broken = true;
        } finally {
            redisDataSource.returnResource(jedis, broken);
        }
        return result;
    }
}