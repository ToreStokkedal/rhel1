/**
 * 
 */
package no.stokkedal.tore.twitterktomongo;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

/**
 * Class that add Tweets to MongoDB, expected used by TweetsFromKafkaToMongo.
 * 
 * @author tore.stokkedal@ibm.com
 *
 */
public class TweetsToMongoDB {

	private Logger log = LoggerFactory.getLogger(this.getClass().getName());

	String mongoHost = "192.168.39.249";
	String databaseName = "thedeveloper";
	String collectionName = "tweets";
	
//	String mongoHost = "192.168.39.249";
//	String database = "thedeveloper";
//	String collectionName = "persons";
	
	MongoClient mongoClient = null;
	MongoDatabase dbDatabase = null;
	MongoCollection<Document> collection = null;

	/**
	 * Iniltialise on localhost, standard port 27017
	 */
	public TweetsToMongoDB() {
		
	}

	/*
	 * Add record
	 */
	public void addRecord(String record) throws Exception{
		try {
			connect();
			Document doc = new Document(Document.parse(record));
			collection.insertOne(doc);
			log.info("Inserted tweet to Mongo" + doc.toString());
		} catch (Exception e) {
			log.error("Error insering record: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Connect with default / configed parameters
	 */
	public void connect(){
		try {
			mongoClient = new MongoClient(mongoHost, 27017);
			dbDatabase = mongoClient.getDatabase(databaseName);
			collection = dbDatabase.getCollection(collectionName);
			log.info("Connected to " + databaseName);

		} catch (Exception e) {
			log.info("Error when opening db" + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		mongoClient.close();
		super.finalize();
	}
}
