package cn.zxw.mongo.util;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class MongoQuery {
	
	/**
	 * Query for All Documents in a Collection
	 */
	public static void find(MongoCollection<Document> collection){
		FindIterable<Document> iterable = collection.find();
		iterable.forEach(new Block<Document>() {
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	/**
	 * Specify Equality Conditions
	 */
	public static void find(MongoCollection<Document> collection,Document document){
		FindIterable<Document> iterable = collection.find();
		iterable.forEach(new Block<Document>() {
		    public void apply(final Document document) {
		        System.out.println(document);
		    }
		});
	}
	
	public static void describeCollection(MongoCollection<Document> collection){
		FindIterable<Document> iterable = collection.find().limit(1);
		iterable.forEach(new Block<Document>() {
		    public void apply(Document document) {
		    	System.out.println(document);
		    }
		});
	}
	
	
}
