package com.emc.nautilus.logclient;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

// Defines a Log output stream.
public abstract class SegmentOutputStream implements AutoCloseable {
	/**
	 * @param buff Data to be written
	 * @param onComplete future to be completed when data has been replicated and stored durrabably.
	 * @return 
	 */
	public abstract void write(ByteBuffer buff, CompletableFuture<Void> onComplete) throws SegmentSealedExcepetion;

	@Override
	public abstract void close() throws SegmentSealedExcepetion;

	public abstract void flush() throws SegmentSealedExcepetion;

	/**
	 * Closes all writing streams associated with this, flushes all outstanding
	 * requests down the socket, and then permanently closes the stream for
	 * appends. Returns the length of the stream after sealing.
	 */
	public abstract Future<Long> seal(long timeoutMillis);
}