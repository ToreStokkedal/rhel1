package no.stokkedal.tore.twitterktomongo.test;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.stokkedal.tore.twitterktomongo.TweetsToMongoDB;
import sun.util.logging.resources.logging;

public class TestTweetstoMongoDB {

	private Logger log = LoggerFactory.getLogger(this.getClass().getName());
	
	public TestTweetstoMongoDB() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {

		TweetsToMongoDB toMongoDB = new TweetsToMongoDB();
		String jsonString = getJSONString();
		try {
			toMongoDB.addRecord(jsonString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Document jsonToBson(String jsonToConvert) {
		return Document.parse(jsonToConvert);

	}

	private static String getJSONString() {
		return "{\"firstname\": \"Tore\", \"lastname\": \"Tore\"} ";
	}

	private JSONObject getJSONString2() {
		JSONObject userDetails1 = new JSONObject();
		userDetails1.put("id", 101);
		userDetails1.put("firstName", "John");
		userDetails1.put("lastName", "Cena");
		userDetails1.put("userName", "John Cena");
		userDetails1.put("email", "john@gmail.com");

		return userDetails1;

	}

}
