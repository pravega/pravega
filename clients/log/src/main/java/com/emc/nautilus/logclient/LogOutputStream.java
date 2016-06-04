package com.emc.nautilus.logclient;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

// Defines a Log output stream.
public abstract class LogOutputStream implements AutoCloseable {
	@FunctionalInterface
	public interface AckListener {
		void ack(long connectionOffset);
	}

	/**
	 * Sets the callback to invoke when acknowledgments arrive for appends. This
	 * method should only be called once.
	 */
	public abstract void setWriteAckListener(AckListener callback);

	/**
	 * @return the connectionOffset. This is the value that will be passed to
	 *         the ack listener when the buffer has been durablably stored.
	 */
	public abstract long write(ByteBuffer buff) throws LogSealedExcepetion;

	@Override
	public abstract void close() throws LogSealedExcepetion;

	public abstract void flush() throws LogSealedExcepetion;

	/**
	 * Closes all writing streams associated with this, flushes all outstanding
	 * requests down the socket, and then permanently closes the stream for
	 * appends. Returns the length of the stream after sealing.
	 */
	public abstract Future<Long> seal(long timeoutMillis);
}