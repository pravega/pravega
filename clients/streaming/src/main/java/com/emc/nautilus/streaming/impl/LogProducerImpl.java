package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.Serializer;

public class LogProducerImpl<Type> implements LogProducer<Type> {

	private final Serializer<Type> serializer;

	private final LogOutputStream out;
	private final ConcurrentSkipListMap<Long, Event<Type>> outstanding = new ConcurrentSkipListMap<>();
	private final AtomicBoolean sealed = new AtomicBoolean(false);
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public LogProducerImpl(LogOutputStream out, Serializer<Type> serializer) {
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
	public void publish(Event<Type> m) throws LogSealedExcepetion {
		checkSealedAndClosed();
		ByteBuffer buffer = serializer.serialize(m.getValue());
		buffer = DataParser.prependLength(buffer);
		long offset = out.write(buffer);			
		outstanding.put(offset, m);
	}

	@Override
	public void flush() throws LogSealedExcepetion {
		checkSealedAndClosed();
		try {
			out.flush();
		} catch (LogSealedExcepetion e) {
			sealed.set(true);
			throw e;
		}
	}

	@Override
	public void close() throws LogSealedExcepetion {
		checkSealed();
		if (closed.get()) {
			return;
		}
		try {
			out.close();
		} catch (LogSealedExcepetion e) {
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
