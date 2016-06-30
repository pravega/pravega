package com.emc.nautilus.common.netty;

public interface ConnectionFactory {

	ClientConnection establishConnection(String endpoint);

	void shutdown();

}
