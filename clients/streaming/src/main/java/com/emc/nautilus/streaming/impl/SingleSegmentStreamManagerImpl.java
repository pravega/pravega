package com.emc.nautilus.streaming.impl;

import java.util.concurrent.ConcurrentHashMap;

import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.client.ConnectionFactoryImpl;
import com.emc.nautilus.logclient.impl.LogClientImpl;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamConfiguration;
import com.emc.nautilus.streaming.StreamManager;

public class SingleSegmentStreamManagerImpl implements StreamManager {

	private final LogClientImpl logClient;
	private final String scope;
	private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
	
	public SingleSegmentStreamManagerImpl(String endpoint, int port, String scope) {
		this.scope = scope;
		ConnectionFactory clientCF = new ConnectionFactoryImpl(false, port);
		this.logClient = new LogClientImpl(endpoint, clientCF);
	}
	
	@Override
	public Stream createStream(String streamName, StreamConfiguration config) {
		boolean existed = created.containsKey(streamName);
		Stream stream = createStreamHelper(streamName,config);
		if (!existed) {
			logClient.createLog(stream.getLatestSegments().getSegments().get(0).getQualifiedName());
		}
		return stream;
	}

	@Override
	public void alterStream(String streamName, StreamConfiguration config) {
			createStreamHelper(streamName, config);
	}

	private Stream createStreamHelper(String streamName, StreamConfiguration config) {
		Stream stream = new SingleSegmentStreamImpl(scope, streamName, config, logClient);
		created.put(streamName, stream);
		return stream;
	}
	
	@Override
	public Stream getStream(String streamName) {
		return created.get(streamName);
	}

}
