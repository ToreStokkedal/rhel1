/**
 * 
 */
package no.stokkedal.tore.learningkafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author torestokkedal
 *
 */
public class KafkaConsumerDemo {
	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	/**
	 * 
	 */
	public KafkaConsumerDemo() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaConsumerDemo demo = new KafkaConsumerDemo();
		demo.run();

	}

	private void run() {

		String bootStrapServer = "127.0.0.1:9092";
		String groupIDString = "my-fourth-application";
		String topic = "first-topic";
		String autoOffsetReset = "earliest";

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIDString);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		// Subscribe¨
		consumer.subscribe(Arrays.asList(topic));
		
		// poll for new
		while(true) {
			ConsumerRecords<String , String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Key " + record.key() + " with Value " + record.value());
			logger.info("Partition; " + record.partition() + ", Offset; " + record.offset());
			}
		}
	}

}
