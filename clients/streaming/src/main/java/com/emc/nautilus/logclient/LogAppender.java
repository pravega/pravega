package com.emc.nautilus.logclient;

import java.util.concurrent.Future;

//Defines methods for operating on a log.
public interface LogAppender extends Appender {
	// Creates a new batch and associates it with this log.
	Batch createBatch(long timeoutMillis);

	// Closes all writing streams associated with this, flushes all outstanding
	// requests down the socket, and then permanently closes the stream for
	// appends.
	// Returns the length of the stream after sealing.
	Future<Long> seal(long timeoutMillis);
}