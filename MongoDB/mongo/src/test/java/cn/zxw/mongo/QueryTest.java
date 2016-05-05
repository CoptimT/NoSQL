package cn.zxw.mongo;

import org.bson.Document;

import cn.zxw.mongo.util.MongoUtil;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class QueryTest{
	public static void main(String[] args) {
		MongoClient client = MongoUtil.getClient("127.0.0.1", 27017);
		MongoDatabase db = MongoUtil.getDB(client, "test");
		MongoCollection<Document> collection = MongoUtil.getCollection(db, "coll");
		find(collection);
		MongoUtil.close(client);
		
	}
	
	public static void find(MongoCollection<Document> collection){
		//FindIterable<Document> iterable = collection.find(new Document("name", "Moda"));//=
		//FindIterable<Document> iterable = collection.find(Filters.eq("name", "Moda"));//=
		//FindIterable<Document> iterable = collection.find(new Document("restaurant_id",new Document("$gt","41704625")));//>
		//FindIterable<Document> iterable = collection.find(Filters.gt("restaurant_id","41704626"));//>
		FindIterable<Document> iterable = collection.find(
				Filters.and(Filters.gte("restaurant_id","41704623"),Filters.lt("restaurant_id","41704627")));//and
		iterable.forEach(new Block<Document>() {
		    public void apply(final Document document) {
		    	String name=document.getString("name");
		        System.out.println(name);
		        System.out.println(document);
		    }
		});
	}
	
	public static void find(MongoDatabase db){
		//db.runCommand();
	}
}
