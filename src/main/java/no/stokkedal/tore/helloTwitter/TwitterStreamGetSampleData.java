package no.stokkedal.tore.helloTwitter;

import java.io.IOException;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Twtter with twitter4jusing Status listener, and is able of diffrentiating in between read, delete etc.
 * @author torestokkedal
 *
 */

public class TwitterStreamGetSampleData {

	public TwitterStreamGetSampleData() {

	}

	public static void main(String[] args) throws TwitterException, IOException {
	
	        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
	        
	        twitterStream.addListener(new StatusListener() {
	            
	        	@Override
	            public void onStatus(Status status) {
	                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
	            }

	            @Override
	            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
	                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
	            }

	            @Override
	            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
	                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
	            }

	            @Override
	            public void onScrubGeo(long userId, long upToStatusId) {
	                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
	            }

	            @Override
	            public void onStallWarning(StallWarning warning) {
	                System.out.println("Got stall warning:" + warning);
	            }

	            @Override
	            public void onException(Exception ex) {
	                ex.printStackTrace();
	            }
	                    
	        });
	        
	      twitterStream.sample("en");
	      // twitterStream.filter(query);
	    }

//	private static void sample() {
//		// TODO Auto-generated method stub
//		
//	}
}

