package com.emc.nautilus.streaming.impl;

import java.util.List;

import com.emc.nautilus.logclient.LogSealedExcepetion;

public interface LogProducer<Type> {
	void publish(Event<Type> m) throws LogSealedExcepetion;
	void flush() throws LogSealedExcepetion; // Block on all outstanding writes.
	void close() throws LogSealedExcepetion;
	boolean isAlreadySealed();
	List<Event<Type>> getUnackedEvents();
}
