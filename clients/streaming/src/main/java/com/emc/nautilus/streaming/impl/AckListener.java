package com.emc.nautilus.streaming.impl;

@FunctionalInterface
public interface AckListener {

	void ack(String eventId);
	
}
