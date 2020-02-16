package no.stokkedal.tore.twitterktomongo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageProducer {

	private Logger log = LoggerFactory.getLogger(this.getClass().getName());
	
	private Properties props = new Properties();
	private String topic = null;
	KafkaProducer<String, String> producer = null;
	
	public KafkaMessageProducer(String bootStrapServer, String topic) {

		this.props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		this.topic = topic;
	}

	public void putMessage(String message) {
		
		try {
			this.producer = new KafkaProducer <String, String>(props);
			{
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
				producer.send(record);
				producer.flush();
			} 			
		} catch (Exception e) {
			log.error(e.toString());
		}

	}
}
