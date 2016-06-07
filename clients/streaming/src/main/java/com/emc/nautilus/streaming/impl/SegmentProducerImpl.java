package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.SegmentOutputStream;
import com.emc.nautilus.logclient.SegmentSealedExcepetion;
import com.emc.nautilus.streaming.Serializer;

public class SegmentProducerImpl<Type> implements SegmentProducer<Type> {

	private final Serializer<Type> serializer;

	private final SegmentOutputStream out;
	private final ConcurrentSkipListMap<Long, Event<Type>> outstanding = new ConcurrentSkipListMap<>();
	private final AtomicBoolean sealed = new AtomicBoolean(false);
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public SegmentProducerImpl(SegmentOutputStream out, Serializer<Type> serializer) {
		this.serializer = serializer;
		this.out = out;
		out.setWriteAckListener((long pos) -> {
			Entry<Long, Event<Type>> entry = outstanding.floorEntry(pos);
			while (entry != null) {
				outstanding.remove(entry.getKey());
				entry.getValue().getCallback().complete(null);
				entry = outstanding.floorEntry(pos);
			}
		});
	}

	@Override
	public void publish(Event<Type> m) throws SegmentSealedExcepetion {
		checkSealedAndClosed();
		ByteBuffer buffer = serializer.serialize(m.getValue());
		long offset = out.write(buffer);			
		outstanding.put(offset, m);
	}

	@Override
	public void flush() throws SegmentSealedExcepetion {
		checkSealedAndClosed();
		try {
			out.flush();
		} catch (SegmentSealedExcepetion e) {
			sealed.set(true);
			throw e;
		}
	}

	@Override
	public void close() throws SegmentSealedExcepetion {
		checkSealed();
		if (closed.get()) {
			return;
		}
		try {
			out.close();
		} catch (SegmentSealedExcepetion e) {
			sealed.set(true);
			throw e;
		}
	}

	private void checkSealed() {
		if (sealed.get()) {
			throw new IllegalStateException("Already sealed");
		}
	}

	private void checkSealedAndClosed() {
		if (sealed.get()) {
			throw new IllegalStateException("Already sealed");
		}
		if (closed.get()) {
			throw new IllegalStateException("out is closed");
		}
	}

	/**
	 * @return All unacked events in the order in which they were published.
	 */
	public List<Event<Type>> getUnackedEvents() {
		return new ArrayList<>(outstanding.values());
	}

	@Override
	public boolean isAlreadySealed() {
		return sealed.get();
	}

}
