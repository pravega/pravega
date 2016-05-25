package com.emc.nautilus.streaming.impl;

import java.util.List;

import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.Transaction;

public interface LogProducer<Type> {
	Transaction<Event<Type>> startTransaction(long timeout) throws LogSealedExcepetion;
	void publish(Event<Type> m) throws LogSealedExcepetion;
	void flush() throws LogSealedExcepetion; // Block on all outstanding writes.
	void close() throws LogSealedExcepetion;
	boolean isAlreadySealed();
	List<Event<Type>> getUnackedEvents();
}
