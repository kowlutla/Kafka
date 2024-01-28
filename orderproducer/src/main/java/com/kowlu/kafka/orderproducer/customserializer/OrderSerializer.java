/**
 * 
 */
package com.kowlu.kafka.orderproducer.customserializer;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author KowlutlaSwamy
 *
 */
public class OrderSerializer implements Serializer<Order> {

	@Override
	public byte[] serialize(String topic, Order order) {
		byte[] response = null;
		ObjectMapper mapper = new ObjectMapper();
		try {
			response = mapper.writeValueAsString(order).getBytes();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return response;
	}

}
