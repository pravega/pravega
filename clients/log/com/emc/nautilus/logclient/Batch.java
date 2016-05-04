package com.emc.nautilus.logclient;

import java.util.concurrent.Future;

//Defines methods for operating on a batch.
public interface Batch extends Appender {
	// Gets the current status of the batch.
	BatchStatus getStatus();

	// Discards the current batch. After this operation is called, no further
	// appends are allowed on the batch.
	void discard();

	// Seals the batch for appends and merges into the parent stream. After this
	// operation is called, no further appends are allowed on the batch.
	Future<Long> mergeIntoParentStream(long timeoutMillis);
}