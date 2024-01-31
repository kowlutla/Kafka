/**
 * 
 */
package com.kowlu.kafka.orderproducer.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author KowlutlaSwamy
 *
 */
public class OrderProducer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				IntegerSerializer.class.getName());
//		props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
		props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
				"order-producer-1");// set transaction id

		KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<String, Integer>(
				props);
		kafkaProducer.initTransactions();// initialize transaction
		ProducerRecord<String, Integer> producerRecord1 = new ProducerRecord<>(
				"OrderTopic", "Dell Latitude", 20);
		ProducerRecord<String, Integer> producerRecord2 = new ProducerRecord<>(
				"OrderTopic", "Iphone", 25);
		ProducerRecord<String, Integer> producerRecord3 = new ProducerRecord<>(
				"OrderTopic", "MacBook Pro", 30);
		try {
			System.out.println("Sending Message....");
			kafkaProducer.beginTransaction();
			kafkaProducer.send(producerRecord1);
			kafkaProducer.send(producerRecord2);
			kafkaProducer.send(producerRecord3);
			kafkaProducer.commitTransaction();
			System.out.println("Message sent successfully...");

		} catch (Exception e) {
			kafkaProducer.abortTransaction();
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}
	}

}
