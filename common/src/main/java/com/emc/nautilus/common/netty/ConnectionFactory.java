package com.emc.nautilus.common.netty;

public interface ConnectionFactory {

	Connection establishConnection(String endpoint);
	
}
