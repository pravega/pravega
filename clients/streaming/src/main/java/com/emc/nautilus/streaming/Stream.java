package com.emc.nautilus.streaming;

public interface Stream {
    String getName();

    StreamConfiguration getConfig();

    StreamSegments getSegments(long time);

    StreamSegments getLatestSegments();

    long getRate(long time);

    <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config);

    <T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, Position startingPosition,
            RateChangeListener l);
}
