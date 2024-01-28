/**
 * 
 */
package com.kowlu.kafka.orderproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer",
				"org.apache.kafka.common.serialization.IntegerSerializer");

		KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<String, Integer>(
				props);
		ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(
				"OrderTopic", "MacBook Pro", 10);
		try {
			System.out.println("Sending Message....");
			kafkaProducer.send(producerRecord, new OrderCallBack());
			
		} catch (Exception e) {

		} finally {
			kafkaProducer.close();
		}
	}

}
