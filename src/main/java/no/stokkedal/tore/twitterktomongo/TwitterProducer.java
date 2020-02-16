package no.stokkedal.tore.twitterktomongo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import sun.util.logging.resources.logging;

/**
 * Class sourcing Twitter messages and producing to Kafka.
 * 
 * @author torestokkedal
 * 
 *         Status; On its way to production grade code, need to be more robust
 *         and externalize secrets, Kafka Config and and search parameters
 */

public class TwitterProducer {

	private Logger log = LoggerFactory.getLogger(this.getClass().getName());

	BlockingQueue<String> msgQueue = null;
	BlockingQueue<Event> eventQueue = null;

	// Secrets to authenticate to Twitter ToreS
	String consumerKey = "mmCpTjwwTfTmbe7Zt1Ipdt1tw";
	String consumerSecret = "cBY8BdxfybHeR9eCwAnDVkPjMCeTwWMCJ30C7yeXji5gb0EcdO";
	String token = "944520734-ia3XZDa8ZCgApbS1zRVq5IEBqPK4ECynJEDB5qQT";
	String secret = "9049xgYPznUx0fuzWODQrQsjsYfGLV4Lf9Zi43pjj7YDo";

	// Kafka config
	String bootStrapServer = "127.0.0.1:9092";
	String topic = "tweets-topic";

	public TwitterProducer() {
		// Noting relevant yet
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {

		log.info("Entering run");
		// create twitter client with search words
		List<String> terms = Lists.newArrayList("Kafka", "Høyre", "arbeiderpartiet", "kristelig folkeparti",
				"senterpartiet");
		Client tClient = createTwitterClient(terms);
		tClient.connect(); // process client

		log.info("Creating Kafka Client");
		KafkaProducer<String, String> producer = createKafkaProducer();

		log.info("Starting loop to get tweets and put to Kafka topic " + topic);
		String msg = null;
		while (!tClient.isDone()) {
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);

			} catch (InterruptedException e) {
				log.info("Loop from mesage q representing twitter got interrupted ");
				e.printStackTrace();
				tClient.stop();
			}
			if (msg != null) {
				log.info("Got message " + msg);
				producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							log.error("Colud not send record", e);
						} else {
							log.info("Sendt message to Kafka " + metadata.toString());					
						}
					}

				});
			} // End if(msg not null)
		} // End while
		log.info("End of running");

	}

	/**
	 * Create a Twitter client using object variables for authenticaiton
	 * 
	 * @param get a list of words to that content should be retrieved from Twitter
	 * @return a Twitter client named Hosebird-Client-01
	 */
	private Client createTwitterClient(List<String> twitterTerms) {

		msgQueue = new LinkedBlockingQueue<String>(100000);
		eventQueue = new LinkedBlockingQueue<Event>(1000);

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);

		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(twitterTerms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)).eventMessageQueue(eventQueue); // optional: use this
																									// if you want to
		Client hosebirdClient = builder.build();

		return hosebirdClient;
	}

	/**
	 * Create connection to Kafka - Kafka producer 
	 * 
	 * @return
	 */
	private KafkaProducer<String, String> createKafkaProducer() {

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		return producer;
	}
}
