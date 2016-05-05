package cn.zxw.mongo;

import cn.zxw.mongo.util.MongoUtil;

import com.mongodb.MongoClient;

public class LocalTest {

	public static void main(String[] args) {
		testLocal();
		
		
	}
	
	public static void testLocal() {
		MongoClient client = null;
		try {
			client = MongoUtil.getClient("127.0.0.1",27017);
			MongoUtil.showDatabases(client);
			MongoUtil.showCollections(client, "test");
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			MongoUtil.close(client);
		}
	}
	
}
