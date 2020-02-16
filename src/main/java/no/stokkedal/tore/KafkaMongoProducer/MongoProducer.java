package no.stokkedal.tore.KafkaMongoProducer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoProducer {

	public MongoProducer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		new MongoProducer().run();
	}

	public void run() {
		
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		
		DB database = mongoClient.getDB("myMongoDb");
		
		mongoClient.getDatabaseNames().forEach(System.out::println);
		
		database.createCollection("customers", null);
		
		DBCollection collection = database.getCollection("customers");
		BasicDBObject document = new BasicDBObject();
		document.put("name", "Shubham");
		document.put("company", "Baeldung");
		collection.insert(document);
		

	}
}
