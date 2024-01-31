/**
 * 
 */
package com.kowlu.kafka.orderproducer.custompartitioners;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", OrderSerializer.class.getName());
		props.setProperty("partitioner.class", VIPPartitioner.class.getName());

		KafkaProducer<String, Order> kafkaProducer = new KafkaProducer<String, Order>(
				props);
		Order order = new Order();
		order.setCustomerName("Kowlutla");
		order.setProduct("IPhone");
		order.setQuantity(50);
		ProducerRecord<String, Order> producerRecord = new ProducerRecord<>(
				"OrderPartitionTopic", order.getCustomerName(), order);
		try {
			System.out.println("Sending Message....");
			RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
			System.out.println("Message put to Topic: " + metadata.topic()
					+ " Partition: " + metadata.partition());

		} catch (Exception e) {

		} finally {
			kafkaProducer.close();
		}
	}

}
