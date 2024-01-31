/**
 * 
 */
package com.kowlu.kafka.orderproducer.producerconfigs;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,IntegerSerializer.class.getName());
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");//0 or 1 or all
		props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "64000");
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //snappy, gzip, lz4
		props.setProperty(ProducerConfig.RETRIES_CONFIG, "2"); //How many retries
		props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "2000"); //MilliSeconds
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "6450000"); //memory size for batch in bytes
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000"); //wait Milliseconds before hand-over batch to center thread
		props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "12000"); //wait Milliseconds to get the response from broker

		KafkaProducer<String, Integer> kafkaProducer = new KafkaProducer<String, Integer>(
				props);
		ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>(
				"OrderTopic", "Dell Latitude", 20);
		try {
			System.out.println("Sending Message....");
			RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
			System.out.println("Message sent successfully...");
			System.out.println(metadata.partition());
			System.out.println(metadata.offset());
			
		} catch (Exception e) {

		} finally {
			kafkaProducer.close();
		}
	}

}
