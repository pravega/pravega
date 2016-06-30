package com.emc.nautilus.streaming.impl;

import java.util.List;

import com.emc.nautilus.logclient.SegmentSealedExcepetion;

public interface SegmentProducer<Type> {
	void publish(Event<Type> m) throws SegmentSealedExcepetion;

	void flush() throws SegmentSealedExcepetion; // Block on all outstanding
												 // writes.

	void close() throws SegmentSealedExcepetion;

	boolean isAlreadySealed();

	List<Event<Type>> getUnackedEvents();
}
