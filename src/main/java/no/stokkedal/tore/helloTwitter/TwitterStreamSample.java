package no.stokkedal.tore.helloTwitter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.stokkedal.tore.twitterktomongo.KafkaMessageProducer;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Class to listen to Twitter and put to messagig what is interesting.
 * 
 * @author torestokkedal
 *
 */
public class TwitterStreamSample {
	
	private Logger log = LoggerFactory.getLogger(this.getClass().getName());
	KafkaMessageProducer producer = null;
	
	public TwitterStreamSample(KafkaMessageProducer producer) {
		this.producer = producer;

	}

	public void startListener() throws TwitterException, IOException {

		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

		twitterStream.addListener(new StatusListener() {

			@Override
			public void onStatus(Status status) {
				
				String msg = status.getUser().getScreenName() + "-" + status.getText();
				producer.putMessage(msg);
				log.info(msg);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				log.debug("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				log.debug("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				log.debug("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				log.debug("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				log.error(ex.toString());
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
