package no.stokkedal.tore.helloTwitter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;

/*
 * Twitter with Twitter4J
 */

public class Twitterstatus {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	public Twitterstatus() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		new Twitterstatus().run();
		
	}
	
	private void run() {
		// Connect to twitter	
		try {
			 Twitter twitter = TwitterFactory.getSingleton();
			    List<Status> statuses = twitter.getHomeTimeline();
			    logger.info("Showing home timeline.");
			    for (Status status : statuses) {
			        logger.info(status.getUser().getName() + ":" + status.getText() + status.getId());	
			    } 
			    
		} catch (Exception e) {
			e.printStackTrace();
		
		}
		// Fetch some tweets
		// Publich on Kafka		
	}

}		