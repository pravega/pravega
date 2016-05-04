package com.emc.nautilus.logclient;

import java.io.OutputStream;

//Defines a Log output stream.
public abstract class LogOutputStream extends OutputStream implements AutoCloseable {
	@FunctionalInterface
	public interface AckListener {
		void ack(long connectionOffset);
	}
	
	// Sets the callback to invoke when acknowledgments arrive for appends.
	public abstract void setWriteAckListener(AckListener callback);

	@Override
	public abstract void write(byte[] buff) throws LogSealedExcepetion;

	@Override
	public abstract void write(byte[] buff, int offset, int length) throws LogSealedExcepetion;

	@Override
	public abstract void close() throws LogSealedExcepetion;

	@Override
	public abstract void flush() throws LogSealedExcepetion;
}