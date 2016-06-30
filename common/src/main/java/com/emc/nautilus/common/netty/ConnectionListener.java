package com.emc.nautilus.common.netty;

public interface ConnectionListener extends AutoCloseable {

	void startListening();

	void shutdown();

	@Override
	void close();
}
