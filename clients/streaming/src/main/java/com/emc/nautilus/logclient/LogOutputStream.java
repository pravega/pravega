package com.emc.nautilus.logclient;

import java.nio.ByteBuffer;

//Defines a Log output stream.
public abstract class LogOutputStream implements AutoCloseable {
	@FunctionalInterface
	public interface AckListener {
		void ack(long connectionOffset);
	}
	
	// Sets the callback to invoke when acknowledgments arrive for appends.
	public abstract void setWriteAckListener(AckListener callback);

	/**
	 * @return the connectionOffset. This is the value that will be passed to
	 *         the ack listener when the buffer has been durablably stored.
	 */
	public abstract long write(ByteBuffer buff) throws LogSealedExcepetion;

	@Override
	public abstract void close() throws LogSealedExcepetion;

	public abstract void flush() throws LogSealedExcepetion;
}