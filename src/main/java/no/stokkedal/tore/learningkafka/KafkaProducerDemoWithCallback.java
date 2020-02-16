package no.stokkedal.tore.learningkafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerDemoWithCallback {
	
	Logger logger = LoggerFactory.getLogger (this.getClass().getName());

	public KafkaProducerDemoWithCallback() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {

		KafkaProducerDemoWithCallback k = new KafkaProducerDemoWithCallback();
		k.run();
	}

	public void run() {
		System.out.println("HelloWorld " + new Date().toString() );

		String bootStrapServer = "127.0.0.1:9092";

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (// KafkaProducer
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
			ProducerRecord <String, String> record = new ProducerRecord <String, String>("first_topic", "Hello World!");
			producer.send(record, new Callback() {		
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						logger.error(exception.toString());
					} else {
						logger.info("Sucsessfully sent, received new metadata" +
							" Topic: " + metadata.topic() + 
							" Partition: " + metadata.topic() +
							" Offset: " + metadata.offset());
					}
				}
			});
			producer.flush();
		} // End try on Kafka Consumer
	}
}
