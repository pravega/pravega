package com.emc.nautilus.streaming.impl;

import java.util.concurrent.ConcurrentHashMap;

import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamConfiguration;
import com.emc.nautilus.streaming.StreamManager;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SingleLogStreamManagerImpl implements StreamManager {

	private final String scope;
	private final LogClient logClient;
	private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
	
	@Override
	public Stream createStream(String streamName, StreamConfiguration config) {
		boolean existed = created.containsKey(streamName);
		Stream stream = createStreamHelper(streamName,config);
		if (!existed) {
			logClient.createLog(stream.getLatestLogs().getLogs().get(0).getQualifiedName());
		}
		return stream;
	}

	@Override
	public void alterStream(String streamName, StreamConfiguration config) {
			createStreamHelper(streamName, config);
	}

	private Stream createStreamHelper(String streamName, StreamConfiguration config) {
		Stream stream = new SingleLogStreamImpl(scope, streamName, config, logClient);
		created.put(streamName, stream);
		return stream;
	}
	
	@Override
	public Stream getStream(String streamName) {
		return created.get(streamName);
	}

}
