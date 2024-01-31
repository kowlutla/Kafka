/**
 * 
 */
package com.kowlu.kafka.orderconsumer.custompartitioners;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

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
				StringDeserializer.class.getName());
		props.setProperty("value.deserializer",
				OrderDeserializer.class.getName());
		props.setProperty("group.id", "OrderGroup");

		KafkaConsumer<String, Order> kafkaConsumer = new KafkaConsumer<>(props);
		kafkaConsumer.subscribe(Collections.singletonList("OrderPartitionTopic"));
		ConsumerRecords<String, Order> records = kafkaConsumer
				.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Order> record : records) {
			String customerName = record.key();
			Order order = record.value();
			System.out.println("Customer Name: " + customerName);
			System.out.println("Product : " + order.getProduct());
			System.err.println("Quantity: " + order.getQuantity());
			System.out.println("Partition: "+ record.partition());
		}
		kafkaConsumer.close();
	}

}
