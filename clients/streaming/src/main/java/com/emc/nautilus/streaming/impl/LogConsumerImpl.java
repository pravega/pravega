package com.emc.nautilus.streaming.impl;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Serializer;

public class LogConsumerImpl<Type> implements LogConsumer<Type> {

	private final LogId logId;
	private final LogInputStream in;
	private final Serializer<Type> deserializer;
	private final DataInputStream dataReader;

	LogConsumerImpl(LogId logId, LogInputStream in, Serializer<Type> deserializer) {
		this.logId = logId;
		this.in = in;
		this.dataReader = new DataInputStream(in);
		this.deserializer = deserializer;
	}

	@Override
	public Type getNextEvent(long timeout) throws EndOfLogException {
		try {
			ByteBuffer buffer;
			synchronized (in) {
				int length = dataReader.readInt();
				buffer = ByteBuffer.allocate(length);
				in.read(buffer.array());
			}
			return deserializer.deserialize(buffer);
		} catch (EOFException e) {
			close();
			throw new EndOfLogException();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		try {
			synchronized (in) {
				in.close();
			}
		} catch (IOException e) {
			// TODO: Log and suppress
		}
	}

	@Override
	public LogId getLogId() {
		return logId;
	}
}
