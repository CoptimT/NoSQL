package cn.zxw.mongo.util;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public class MongoInsert {
	
	public static void insert(MongoCollection<Document> collection,Document document){
		collection.insertOne(document);
	}
	
	
}
