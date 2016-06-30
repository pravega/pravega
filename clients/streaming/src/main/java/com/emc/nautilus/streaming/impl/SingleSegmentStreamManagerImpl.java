package com.emc.nautilus.streaming.impl;

import java.util.concurrent.ConcurrentHashMap;

import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.client.ConnectionFactoryImpl;
import com.emc.nautilus.logclient.impl.LogServiceClientImpl;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamConfiguration;
import com.emc.nautilus.streaming.StreamManager;

public class SingleSegmentStreamManagerImpl implements StreamManager {

	private final LogServiceClientImpl logServiceClient;
	private final String scope;
	private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
	private final ConnectionFactory clientCF;

	public SingleSegmentStreamManagerImpl(String endpoint, int port, String scope) {
		this.scope = scope;
		this.clientCF = new ConnectionFactoryImpl(false, port);
		this.logServiceClient = new LogServiceClientImpl(endpoint, clientCF);
	}

	@Override
	public Stream createStream(String streamName, StreamConfiguration config) {
		boolean existed = created.containsKey(streamName);
		Stream stream = createStreamHelper(streamName, config);
		if (!existed) {
			logServiceClient.createSegment(stream.getLatestSegments().getSegments().get(0).getQualifiedName());
		}
		return stream;
	}

	@Override
	public void alterStream(String streamName, StreamConfiguration config) {
		createStreamHelper(streamName, config);
	}

	private Stream createStreamHelper(String streamName, StreamConfiguration config) {
		Stream stream = new SingleSegmentStreamImpl(scope, streamName, config, logServiceClient);
		created.put(streamName, stream);
		return stream;
	}

	@Override
	public Stream getStream(String streamName) {
		return created.get(streamName);
	}

	@Override
	public void shutdown() {
		clientCF.shutdown();
	}

}
