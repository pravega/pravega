package com.emc.nautilus.demo;

import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.impl.JavaSerializer;
import com.emc.nautilus.streaming.impl.SingleSegmentStreamManagerImpl;

import lombok.Cleanup;

public class StartProducer {

	public static void main(String[] args) {
		String endpoint = "localhost";
		int port = 12345;
		String scope = "Scope1";
		String streamName = "Stream1";
		String testString = "Hello world: ";
		@Cleanup("shutdown")
		SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(endpoint, port, scope);
		Stream stream = streamManager.createStream(streamName, null);
		@Cleanup
		Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
		for (int i = 0; i < 10000; i++) {
			producer.publish(null, testString + i + "\n");
		}
		producer.flush();
	}

}
