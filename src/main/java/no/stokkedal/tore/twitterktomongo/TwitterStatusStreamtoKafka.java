package no.stokkedal.tore.twitterktomongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.stokkedal.tore.helloTwitter.TwitterStreamSample;

public class TwitterStatusStreamtoKafka {

	private Logger log = LoggerFactory.getLogger(this.getClass().getName());
	
	private String kafkaBootStrapServer = "127.0.0.1:9092";
	private String topic = "twitter-status-sample";
	
	public TwitterStatusStreamtoKafka() {
		// Make Producer object
		log.info("Entering");
		
		KafkaMessageProducer producer = new KafkaMessageProducer(kafkaBootStrapServer, topic);
		log.info("Made kafka producer" + producer.toString());
		
		try {
			// Make Twitter listener, and pass producer
			TwitterStreamSample twitterStream = new TwitterStreamSample(producer);
			twitterStream.startListener();			
		} catch (Exception e) {
			log.error("Tried to create Twitter stream, got " + e);
		}

	}

	public static void main(String[] args) {
		// Construction will start a permanent listener
		new TwitterStatusStreamtoKafka();
	}
}
