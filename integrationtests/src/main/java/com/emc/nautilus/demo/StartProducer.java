package com.emc.nautilus.demo;

import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.client.ConnectionFactoryImpl;
import com.emc.nautilus.logclient.impl.LogClientImpl;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.impl.JavaSerializer;
import com.emc.nautilus.streaming.impl.SingleLogStreamManagerImpl;

import lombok.Cleanup;

public class StartProducer {
	
	public static final int PORT = 12345;

	public static void main(String[] args) {
		String endpoint = args[0];
		String streamName = "abc";
		String testString = "Hello world: ";
		
		ConnectionFactory clientCF = new ConnectionFactoryImpl(false, PORT);
		LogClientImpl logClient = new LogClientImpl(endpoint, clientCF);
		SingleLogStreamManagerImpl streamManager = new SingleLogStreamManagerImpl("Scope", logClient);
		Stream stream = streamManager.createStream(streamName, null);
		@Cleanup
		Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
		for (int i=0;i<1000;i++) {
			producer.publish("RoutingKey", testString+i+"\n");
		}
		producer.flush();
	}

}
