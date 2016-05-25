package com.emc.nautilus.streaming;

public interface Stream {
	String getName();
	StreamConfiguration getConfig();
	StreamLogs getLogs(long time);
	StreamLogs getLatestLogs();
	long getRate(long time);
    <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config);
	<T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, 
			Position startingPosition, RateChangeListener l);
}
