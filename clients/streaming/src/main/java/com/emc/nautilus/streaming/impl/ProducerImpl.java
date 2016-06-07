package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.LogAppender;
import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.EventRouter;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamSegments;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class ProducerImpl<Type> implements Producer<Type> {

	private final Stream stream;
	private final Serializer<Type> serializer;
	private final LogClient logClient;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final EventRouter router;
	private final ProducerConfig config;
	private final Map<SegmentId, LogProducer<Type>> producers = new HashMap<>();

	ProducerImpl(Stream stream, LogClient logClient, EventRouter router, Serializer<Type> serializer,
			ProducerConfig config) {
		this.logClient = logClient;
		this.stream = stream;
		this.router = router;
		this.serializer = serializer;
		this.config = config;
		List<Event<Type>> list = setupLogProducers();
		if (!list.isEmpty()) {
			throw new IllegalStateException("Producer initialized with unsent messages?!");
		}
	}

	private List<Event<Type>> setupLogProducers() {
		StreamSegments logs = stream.getLatestSegments();
		List<SegmentId> newLogs = new ArrayList<>(logs.segments);
		newLogs.removeAll(producers.keySet());
		List<SegmentId> oldLogs = new ArrayList<>(producers.keySet());
		oldLogs.removeAll(logs.segments);

		for (SegmentId l : newLogs) {
			LogAppender log = logClient.openLogForAppending(l.getQualifiedName(), config.getSegmentConfig());
			producers.put(l, new LogProducerImpl<Type>(log, serializer));
		}
		List<Event<Type>> toResend = new ArrayList<>();
		for (SegmentId l : oldLogs) {
			LogProducer<Type> producer = producers.remove(l);
			try {
				producer.close();
			} catch (LogSealedExcepetion e) {
				// Suppressing expected exception
			}
			toResend.addAll(producer.getUnackedEvents());
		}
		return toResend;
	}

	@Override
	public Future<Void> publish(String routingKey, Type event) {
		if (closed.get()) {
			throw new IllegalStateException("Producer closed");
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		synchronized (producers) {
			if (!attemptPublish(new Event<Type>(routingKey, event, result))) {
				handleLogSealed();
			}
		}
		return result;
	}

	private void handleLogSealed() {
		List<Event<Type>> toResend = setupLogProducers();
		while (toResend.isEmpty()) {
			List<Event<Type>> unsent = new ArrayList<>();
			for (Event<Type> event : toResend) {
				if (!attemptPublish(event)) {
					unsent.add(event);
				}
			}
			if (!unsent.isEmpty()) {
				unsent.addAll(setupLogProducers());
			}
			toResend = unsent;
		}
	}

	private boolean attemptPublish(Event<Type> event) {
		LogProducer<Type> log = getLogProducer(event.getRoutingKey());
		if (log == null || log.isAlreadySealed()) {
			return false;
		}
		try {
			log.publish(event);
			return true;
		} catch (LogSealedExcepetion e) {
			return false;
		}
	}

	private LogProducer<Type> getLogProducer(String routingKey) {
		SegmentId log = router.getSegmentForEvent(stream, routingKey);
		return producers.get(log);
	}

	private static class TransactionImpl<Type> implements Transaction<Type> {

		final Transaction<Event<Type>> inner;
		private String routingKey;

		TransactionImpl(String routingKey, Transaction<Event<Type>> transaction) {
			this.routingKey = routingKey;
			this.inner = transaction;
		}

		@Override
		public void publish(Type event) throws TxFailedException {
			inner.publish(new Event<Type>(routingKey, event, null));
		}

		@Override
		public void commit() throws TxFailedException {
			inner.commit();
		}

		@Override
		public void drop() {
			inner.drop();
		}

		@Override
		public Status checkStatus() {
			return inner.checkStatus();
		}

	}

	@Override
	public Transaction<Type> startTransaction(String routingKey, long timeout) {
		Transaction<Event<Type>> transaction = null;
		while (transaction == null) {
			synchronized (producers) {
				LogProducer<Type> logProducer = getLogProducer(routingKey);
				if (logProducer != null) {
					try {
						transaction = logProducer.startTransaction(timeout);
					} catch (LogSealedExcepetion e) {
						// Ignore
					}
				}
				if (transaction == null) {
					handleLogSealed();
				}
			}
		}
		return new TransactionImpl<Type>(routingKey, transaction);
	}

	@Override
	public void flush() {
		if (closed.get()) {
			throw new IllegalStateException("Producer closed");
		}
		boolean success = false;
		while (!success) {
			success = true;
			synchronized (producers) {
				for (LogProducer<Type> p : producers.values()) {
					try {
						p.flush();
					} catch (LogSealedExcepetion e) {
						success = false;
					}
				}
				if (!success) {
					handleLogSealed();
				}
			}
		}
	}

	@Override
	public void close() {
		if (closed.getAndSet(true)) {
			return;
		}
		synchronized (producers) {
			boolean success = false;
			while (!success) {
				success = true;
				for (LogProducer<Type> p : producers.values()) {
					try {
						p.close();
					} catch (LogSealedExcepetion e) {
						success = false;
					}
				}
				if (!success) {
					handleLogSealed();
				}
			}
			producers.clear();
		}
	}

	@Override
	public ProducerConfig getConfig() {
		return config;
	}

}
