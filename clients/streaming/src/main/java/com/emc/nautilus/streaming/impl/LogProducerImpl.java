package com.emc.nautilus.streaming.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.Batch;
import com.emc.nautilus.logclient.LogAppender;
import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class LogProducerImpl<Type> implements LogProducer<Type> {

	private static final int REQUEST_TIMEOUT = 60000;
	private final Serializer<Type> serializer;

	private final LogAppender log;
	private final LogOutputStream out;
	private final ConcurrentSkipListMap<Long, Event<Type>> outstanding = new ConcurrentSkipListMap<>();
	private final AtomicBoolean sealed = new AtomicBoolean(false);
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public LogProducerImpl(LogAppender log, Serializer<Type> serializer) {
		this.log = log;
		this.serializer = serializer;
		this.out = log.getOutputStream();
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

	static final class TransactionImpl<Type> implements Transaction<Event<Type>> {
		private final Serializer<Type> serializer;
		private final Batch batch;
		private final LogOutputStream out;

		TransactionImpl(Batch batch, Serializer<Type> serializer) {
			this.batch = batch;
			this.out = batch.getOutputStream();
			this.serializer = serializer;
		}

		@Override
		public void publish(Event<Type> event) throws TxFailedException {
			try {
				ByteBuffer buffer = serializer.serialize(event.getValue());
				buffer = DataParser.prependLength(buffer);
				out.write(buffer);
			} catch (LogSealedExcepetion e) {
				throw new TxFailedException();
			}
		}

		@Override
		public void commit() throws TxFailedException {
			try {
				out.flush();
			} catch (IOException e) {
				throw new TxFailedException();
			}
			batch.mergeIntoParentStream(REQUEST_TIMEOUT);
		}

		@Override
		public void drop() {
			batch.discard();
		}

		@Override
		public Status checkStatus() {
			switch (batch.getStatus()) {
			case DROPPED:
				return Status.DROPPED;
			case MERGED:
				return Status.COMMITTED;
			case OPEN:
				return Status.OPEN;
			default:
				throw new IllegalStateException("Unknown status: " + batch.getStatus());
			}
		}
	}

	@Override
	public Transaction<Event<Type>> startTransaction(long timeout) throws LogSealedExcepetion {
		checkSealedAndClosed();
		try {
			flush();
		} catch (LogSealedExcepetion e) {
			sealed.set(true);
		}
		return new TransactionImpl<>(log.createBatch(timeout), serializer);
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
