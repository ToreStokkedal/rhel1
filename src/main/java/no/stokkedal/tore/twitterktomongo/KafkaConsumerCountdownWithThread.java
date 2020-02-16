/**
 * 
 */
package no.stokkedal.tore.twitterktomongo;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tore.stokkedal@ibm.com
 * Class as safe Consumer of Kafka topic.
 * 
 * @param Bootstrap server is hard coded
 * @param groupID - topic is hard coded
 * @param topic is hard coded
 */
public class KafkaConsumerCountdownWithThread {
	private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String bootStrapServer = "127.0.0.1:9092";
		String groupID = "my-eight-application";
		String topic = "first-topic";
		
		new KafkaConsumerCountdownWithThread().run(bootStrapServer, groupID, topic);		
	}
	
	public void run(String bootStrapServer, String groupID, String topic ) {
		
		logger.info("Entering with bootstrap" + bootStrapServer + "GroupID, applicationID: " + groupID + " topic " + topic);

		// Dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		
		// Create the class that does the Kafka work
		Runnable myConsumerRunnable = new KafkaConsumerRunnable(latch, 
				bootStrapServer, 
				groupID,
				topic);
		
		// Start a thread with the runnable
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Adding shutdown hook");
			((KafkaConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (Exception e) {
				e.printStackTrace();
			}
			logger.info("Application exited");
			}
		));
		
		// Wait while the runnable is working, when interrupted, handle the execption and exit
		try {
			latch.await();			
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing!");
		}
	}
}
