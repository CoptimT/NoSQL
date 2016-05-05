package cn.zxw.mongo;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;

import org.bson.Document;

import cn.zxw.mongo.util.MongoUtil;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

public class InsertTest {
	public static void main(String[] args) throws Exception {
		MongoClient client = MongoUtil.getClient("127.0.0.1", 27017);
		MongoCollection<Document> collection = MongoUtil.getCollection(client, "test", "coll");
		insert(collection);
		MongoUtil.close(client);
	}
	
	public static void insert(MongoCollection<Document> coll) throws ParseException{
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);
		Document document = new Document("address",new Document()
														.append("street", "2 Avenue")
													    .append("zipcode", "10075")
													    .append("building", "1480")
													    .append("coord", Arrays.asList(-73.9557413, 40.7720266)))
						.append("borough", "Manhattan")
						.append("cuisine", "Italian")
				        .append("grades", Arrays.asList(
				        		    new Document()
										.append("date", format.parse("2014-10-01T00:00:00Z"))
										.append("grade", "A")
										.append("score", 11),
									new Document()
										.append("date", format.parse("2014-01-16T00:00:00Z"))
										.append("grade", "B")
										.append("score", 17)))
						.append("name", "llily")
						.append("restaurant_id", "41704623");
		coll.insertOne(document);
	}
	
}
