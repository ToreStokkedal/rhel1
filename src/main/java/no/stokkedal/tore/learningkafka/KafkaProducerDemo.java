package no.stokkedal.tore.learningkafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {

	public static void main(String[] args) {

		KafkaProducerDemo k = new KafkaProducerDemo();
		k.run();
	}

	public void run() {
		String bootStrapServer = "127.0.0.1:9092";

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (// KafkaProducer
				KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
			ProducerRecord <String, String> record = new ProducerRecord <String, String>("first-topic", "Hello World !");

			producer.send(record);
			
			producer.flush();
		}
	}
}
