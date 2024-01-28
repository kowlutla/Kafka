/**
 * 
 */
package com.kowlu.kafka.orderconsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author KowlutlaSwamy
 *
 */
public class OrderConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer",
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.setProperty("group.id", "OrderGroup");

		KafkaConsumer<String, Integer> kafkaConsumer = new KafkaConsumer<String, Integer>(
				props);
		kafkaConsumer.subscribe(Collections.singletonList("OrderTopic"));
		ConsumerRecords<String, Integer> orders = kafkaConsumer
				.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Integer> order : orders) {
			System.out.println("Product Name: " + order.key());
			System.out.println("Quantity: " + order.value());
		}
		kafkaConsumer.close();
	}

}
