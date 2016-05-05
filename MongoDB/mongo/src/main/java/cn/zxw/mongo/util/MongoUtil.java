package cn.zxw.mongo.util;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MongoUtil {
	
	public static MongoClient getClient(String host,int port){
		MongoClient client = new MongoClient(host, port);
		return client;
	}
	
	public static void close(MongoClient client){
		if(client != null){
			client.close();
		}
	}
	
	//client cannot close
	/*public static MongoDatabase getDB(String host,int port,String dbName){
		MongoDatabase db = getClient(host,port).getDatabase(dbName);
		return db;
	}*/
	public static MongoDatabase getDB(MongoClient client,String dbName){
		MongoDatabase db = client.getDatabase(dbName);
		return db;
	}
	
	//client cannot close
	/*public static MongoCollection<Document> getCollection(String host,int port,String dbName,String collName){
		MongoCollection<Document> collection = getDB(host,port,dbName).getCollection(collName);
		return collection;
	}*/
	public static MongoCollection<Document> getCollection(MongoClient client,String dbName,String collName){
		MongoCollection<Document> collection = getDB(client, dbName).getCollection(collName);
		return collection;
	}
	public static MongoCollection<Document> getCollection(MongoDatabase db,String collName){
		MongoCollection<Document> collection = db.getCollection(collName);
		return collection;
	}
	
	public static MongoCursor<String> listDatabases(MongoClient client){
		MongoIterable<String> dbitera = client.listDatabaseNames();
		MongoCursor<String> dbs = dbitera.iterator();
		return dbs;
	}
	public static void showDatabases(MongoClient client){
		MongoCursor<String> dbs = listDatabases(client);
		while(dbs.hasNext()){
			System.out.println(dbs.next());
		}
	}
	
	public static MongoCursor<String> listCollections(MongoClient client,String dbName){
		MongoDatabase db = client.getDatabase(dbName);
		MongoIterable<String> collitera = db.listCollectionNames();
		MongoCursor<String> colls = collitera.iterator();
		return colls;
	}
	public static void showCollections(MongoClient client,String dbName){
		MongoCursor<String> colls = listCollections(client,dbName);
		while(colls.hasNext()){
			System.out.println(colls.next());
		}
	}
	
	
	
	
}
