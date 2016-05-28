package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;

import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Serializer;

public class LogConsumerImpl<Type> implements LogConsumer<Type> {

	private final LogId logId;
	private final LogInputStream in;
	private final Serializer<Type> deserializer;

	LogConsumerImpl(LogId logId, LogInputStream in, Serializer<Type> deserializer) {
		this.logId = logId;
		this.in = in;
		this.deserializer = deserializer;
	}

	@Override
	public Type getNextEvent(long timeout) throws EndOfLogException {
		ByteBuffer buffer;
		synchronized (in) { //TODO: This implementation sucks.
			buffer = ByteBuffer.allocate(4);
			in.read(buffer);
			int length = buffer.getInt();
			buffer = ByteBuffer.allocate(length);
			in.read(buffer);
		}
		return deserializer.deserialize(buffer);
	}

	@Override
	public long getOffset() {
		synchronized (in) {
			return in.getOffset();
		}
	}

	@Override
	public void setOffset(long offset) {
		synchronized (in) {
			in.setOffset(offset);
		}
	}

	@Override
	public void close() {
		synchronized (in) {
			in.close();
		}
	}

	@Override
	public LogId getLogId() {
		return logId;
	}
}
