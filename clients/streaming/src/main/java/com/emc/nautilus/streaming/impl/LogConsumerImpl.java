package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;

import com.emc.nautilus.logclient.EndOfSegmentException;
import com.emc.nautilus.logclient.SegmentInputStream;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Serializer;

public class LogConsumerImpl<Type> implements SegmentConsumer<Type> {

	private final SegmentId segmentId;
	private final SegmentInputStream in;
	private final Serializer<Type> deserializer;

	LogConsumerImpl(SegmentId segmentId, SegmentInputStream in, Serializer<Type> deserializer) {
		this.segmentId = segmentId;
		this.in = in;
		this.deserializer = deserializer;
	}

	@Override
	public Type getNextEvent(long timeout) throws EndOfSegmentException {
		ByteBuffer buffer;
		synchronized (in) { // TODO: This implementation sucks.
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
	public SegmentId getLogId() {
		return segmentId;
	}
}
