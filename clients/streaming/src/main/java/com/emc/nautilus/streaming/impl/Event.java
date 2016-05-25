package com.emc.nautilus.streaming.impl;

import java.util.concurrent.CompletableFuture;

public class Event<Type> { //TODO Work out what we want to do with Generics. Does this class need to exist?

	private final Type value;
	private final String routingKey;
	private final CompletableFuture<Void> ackFuture;
	// TODO is there a better way to do this?

	public Event(String routingKey, Type value, CompletableFuture<Void> ackFuture) {
		this.routingKey = routingKey;
		this.value = value;
		this.ackFuture = ackFuture;	
	}
	public Type getValue() {
		return value;
	}
	public String getRoutingKey() {
		return routingKey;
	}
	public CompletableFuture<Void> getCallback() {
		return ackFuture;
	}
	//TODO: fast hash code/equals

}
