package com.emc.nautilus.streaming.impl;

import java.util.Collections;

import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.ConsumerConfig;
import com.emc.nautilus.streaming.EventRouter;
import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Position;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.RateChangeListener;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamConfiguration;
import com.emc.nautilus.streaming.StreamLogs;

import lombok.Getter;

public class SingleLogStreamImpl implements Stream {

	private final String scope;
	@Getter
	private final String name;
	@Getter
	private final StreamConfiguration config;
	private final LogId logId;
	private final LogClient logClient;
	private final EventRouter router = new EventRouter() {
		@Override
		public LogId getLogForEvent(Stream stream, String routingKey) {
			return logId;
		}
	};
	
	public SingleLogStreamImpl(String scope, String name, StreamConfiguration config, LogClient logClient) {
		this.scope = scope;
		this.name = name;
		this.config = config;
		this.logClient = logClient;
		this.logId = new LogId(scope, name, 1, 0);
	}
	

	@Override
	public StreamLogs getLogs(long time) {
		return new StreamLogs(Collections.singletonList(logId), time);
	}

	@Override
	public StreamLogs getLatestLogs() {
		return getLogs(System.currentTimeMillis());
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
		//return new ConsumerImpl<>(this, logClient, s, startingPosition, orderer, l, config);
	}

}
