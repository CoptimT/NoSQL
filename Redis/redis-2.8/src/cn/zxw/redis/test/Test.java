package cn.zxw.redis.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Test {
	public static JedisPoolConfig config=null;
	public static JedisPool pool=null;
	
	public static void main(String[] args) {
		config=new JedisPoolConfig();
		config.setMaxActive(5);
		config.setMaxIdle(3);
		config.setMaxWait(10);
		config.setTestOnBorrow(true);
		config.setTestOnReturn(true);
		
		pool=new JedisPool(config, "192.168.1.203", 6379, 30000, "rtmap@2014");
        
		for(int i=14;i<24;i++){
			String imei = hget("0000000000" + i, "imei", 0);
			String mac = hget("0000000000" + i, "mac", 0);
			String brand = hget("0000000000" + i, "brand", 0);
			String idfa = hget("0000000000" + i, "idfa", 0);
			System.out.println(imei + " , " + mac + " , " + brand + " , " + idfa);
		}
		
		pool.destroy();
	}
	
	public static String hget(String key, String field,int dbIndex) {
        String result = null;
        Jedis jedis = getRedisClient();
        if (jedis == null) {
            return result;
        }
        jedis.select(dbIndex);
        boolean broken = false;
        try {
            result = jedis.hget(key, field);
        } catch (Exception e) {
        	broken = true;
            System.out.println(e.getMessage());
        } finally {
            returnResource(jedis, broken);
        }
        return result;
    }

	 public static Jedis getRedisClient() {
		try {
			Jedis jedis = pool.getResource();
			return jedis;
		} catch (Exception e) {
			System.out.println("getRedisClent error:" + e.getLocalizedMessage());
		}
		return null;
	}
	 
	 public static void returnResource(Jedis jedis, boolean broken) {
		if (broken) {
			pool.returnBrokenResource(jedis);
		} else {
			pool.returnResource(jedis);
		}
	}
	 
	 
}
