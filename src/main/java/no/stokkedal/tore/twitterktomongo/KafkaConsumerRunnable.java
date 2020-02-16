package no.stokkedal.tore.twitterktomongo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Good implementation of Kafka consumer, with graceful exit, exeption handling etc. Its from Oreillys Learn Kafka for beginners, and @
 * tweaked by tore.s
 */

public class KafkaConsumerRunnable implements Runnable {

	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	private CountDownLatch latch = null;
	KafkaConsumer<String, String> consumer = null;
	TweetsToMongoDB tweetsToMongoDB = null;

	/*
	 * Constructor, setup offset,  
	 * @param
	 */
	public KafkaConsumerRunnable(CountDownLatch latch, String bootStrapSerever, String groupID, String topics) {

		String autoOffsetReset = "earliest";

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapSerever);
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

		this.latch = latch;
		this.consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topics));
		
		tweetsToMongoDB = new TweetsToMongoDB();
	}

	/*
	 * 
	 */
	@Override
	public void run(){
		// Poll for new data
		try {
			logger.info("Entering into Run, starting to Poll on topic" + consumer);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					logger.info("Key " + record.key() + " with Value " + record.value());
					logger.info("Partition; " + record.partition() + ", Offset; " + record.offset());
					tweetsToMongoDB.addRecord(record.value());
				}
			}
		} catch (WakeupException e) {
			// Wakeup is normal behavior when .. thread try to terminate
			logger.info("Received shutwown signal!");
		} catch (Exception e) {
			logger.error(e.toString());
		} 
		finally {
			consumer.close();
			latch.countDown();
		}	
	}

	
	/*
	 * 
	 */
	public void shutdown() {
		// interrupt consumer.poll
		// It will throw a WakeUp Exception and disrupt the while(true) below
		consumer.wakeup();
		
	}

}
