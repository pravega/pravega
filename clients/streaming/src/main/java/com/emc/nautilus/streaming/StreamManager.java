package com.emc.nautilus.streaming;

public interface StreamManager {
	Stream createStream(String streamName, StreamConfiguration config);

	void alterStream(String streamName, StreamConfiguration config);

	Stream getStream(String streamName);

	void shutdown();
}
