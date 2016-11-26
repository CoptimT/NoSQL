package cn.zxw.redis;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class ReidsOutput {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		Jedis jedis;
		Pipeline pipeline;
		
		jedis = new Jedis("", 888, 60000);
		pipeline = jedis.pipelined();
		
		// 测试jedis的批量写入
		pipeline.select(("".hashCode() & Integer.MAX_VALUE) % 32);
		pipeline.set("key", "value");
		int rowCount = 0;
		if (rowCount % 10000 == 0) {
			pipeline.sync();
		}
		if(pipeline != null){
			pipeline.close();
		}
		if (jedis != null){
			jedis.disconnect();
		}
		
	}

}
