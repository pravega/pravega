package com.emc.nautilus.streaming.impl;

import java.util.Collections;

import com.emc.nautilus.logclient.LogServiceClient;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.ConsumerConfig;
import com.emc.nautilus.streaming.EventRouter;
import com.emc.nautilus.streaming.Position;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.RateChangeListener;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamConfiguration;
import com.emc.nautilus.streaming.StreamSegments;

import lombok.Getter;

public class SingleSegmentStreamImpl implements Stream {

    private final String scope;
    @Getter
    private final String name;
    @Getter
    private final StreamConfiguration config;
    private final SegmentId logId;
    private final LogServiceClient logClient;
    private final EventRouter router = new EventRouter() {
        @Override
        public SegmentId getSegmentForEvent(Stream stream, String routingKey) {
            return logId;
        }
    };

    public SingleSegmentStreamImpl(String scope, String name, StreamConfiguration config, LogServiceClient logClient) {
        this.scope = scope;
        this.name = name;
        this.config = config;
        this.logClient = logClient;
        this.logId = new SegmentId(scope, name, 1, 0);
    }

    @Override
    public StreamSegments getSegments(long time) {
        return new StreamSegments(Collections.singletonList(logId), time);
    }

    @Override
    public StreamSegments getLatestSegments() {
        return getSegments(System.currentTimeMillis());
    }

    @Override
    public long getRate(long time) {
        return 0;
    }

    @Override
    public <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config) {
        return new ProducerImpl<T>(null, this, logClient, router, s, config);
    }

    @Override
    public <T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, Position startingPosition,
            RateChangeListener l) {
        return null;
        // return new ConsumerImpl<>(this, logClient, s, startingPosition,
        // orderer, l, config);
    }

}
