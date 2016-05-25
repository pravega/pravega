package com.emc.nautilus.streaming;

public interface EventRouter {

	LogId getLogForEvent(Stream stream, String routingKey);
	
}
