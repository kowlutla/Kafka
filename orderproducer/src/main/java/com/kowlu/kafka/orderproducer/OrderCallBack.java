/**
 * 
 */
package com.kowlu.kafka.orderproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author KowlutlaSwamy
 *
 */
public class OrderCallBack implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println("Message sent successfully...");
		System.out.println(metadata.partition());
		System.out.println(metadata.offset());
		if(exception!=null) {
			System.out.println(exception.getMessage());
		}
	}

}
