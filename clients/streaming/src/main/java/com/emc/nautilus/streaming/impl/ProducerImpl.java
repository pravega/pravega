package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.EventRouter;
import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamLogs;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class ProducerImpl<Type> implements Producer<Type> {

	private final TransactionManager txManager;
	private final Stream stream;
	private final Serializer<Type> serializer;
	private final LogClient logClient;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final EventRouter router;
	private final ProducerConfig config;
	private final Map<LogId, LogProducer<Type>> producers = new HashMap<>();

	ProducerImpl(TransactionManager txManager, Stream stream, LogClient logClient, EventRouter router,
			Serializer<Type> serializer, ProducerConfig config) {
		this.txManager = txManager;
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
		StreamLogs logs = stream.getLatestLogs();
		List<LogId> newLogs = new ArrayList<>(logs.logs);
		newLogs.removeAll(producers.keySet());
		List<LogId> oldLogs = new ArrayList<>(producers.keySet());
		oldLogs.removeAll(logs.logs);

		for (LogId l : newLogs) {
			LogOutputStream log = logClient.openLogForAppending(l.getQualifiedName(), config.getLogConfig());
			producers.put(l, new LogProducerImpl<Type>(log, serializer));
		}
		List<Event<Type>> toResend = new ArrayList<>();
		for (LogId l : oldLogs) {
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
		LogId log = router.getLogForEvent(stream, routingKey);
		return producers.get(log);
	}

	private class TransactionImpl implements Transaction<Type> {

		private final Map<LogId, LogTransaction<Type>> inner;
		private UUID txId;

		TransactionImpl(UUID txId, Map<LogId, LogTransaction<Type>> transactions) {
			this.txId = txId;
			this.inner = transactions;
		}

		@Override
		public void publish(String routingKey, Type event) throws TxFailedException {
			LogId log = router.getLogForEvent(stream, routingKey);
			LogTransaction<Type> transaction = inner.get(log);
			transaction.publish(event);
		}

		@Override
		public void commit() throws TxFailedException {
			for (LogTransaction<Type> log : inner.values()) {
				log.flush();
			}
			txManager.commitTransaction(txId);
		}

		@Override
		public void drop() {
			txManager.dropTransaction(txId);
		}

		@Override
		public Status checkStatus() {
			return txManager.checkTransactionStatus(txId);
		}

	}

	@Override
	public Transaction<Type> startTransaction(long timeout) {
		UUID txId = txManager.createTransaction(stream, timeout);
		Map<LogId, LogTransaction<Type>> transactions = new HashMap<>();
		ArrayList<LogId> logIds;  
		synchronized (producers) {
			logIds = new ArrayList<>(producers.keySet());
		}
		for (LogId log : logIds) {
			LogOutputStream out = logClient.openTransactionForAppending(log.getName(), txId);
			LogTransactionImpl<Type> impl = new LogTransactionImpl<>(txId, out, serializer);
			transactions.put(log, impl);
		}
		return new TransactionImpl(txId, transactions);
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
