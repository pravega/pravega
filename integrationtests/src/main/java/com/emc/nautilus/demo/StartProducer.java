package com.emc.nautilus.demo;

import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.impl.JavaSerializer;
import com.emc.nautilus.streaming.impl.SingleSegmentStreamManagerImpl;

import lombok.Cleanup;

public class StartProducer {
	
	public static final int PORT = 12345;

	public static void main(String[] args) {
		String endpoint = "localhost";
		String streamName = "abc";
		String testString = "Hello world: ";

		SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(endpoint, PORT, "Scope1");
		Stream stream = streamManager.createStream(streamName, null);
		@Cleanup
		Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
		for (int i=0;i<1000;i++) {
			producer.publish("RoutingKey", testString+i+"\n");
		}
		producer.flush();
	}

}
