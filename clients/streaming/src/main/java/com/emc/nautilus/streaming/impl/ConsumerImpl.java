package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogClient;
import com.emc.nautilus.logclient.LogInputStream;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.ConsumerConfig;
import com.emc.nautilus.streaming.LogId;
import com.emc.nautilus.streaming.Position;
import com.emc.nautilus.streaming.RateChangeListener;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;

public class ConsumerImpl<Type> implements Consumer<Type> {

	private final Serializer<Type> deserializer;
	private final LogClient logClient;

	private final Stream stream;
	private final Orderer<Type> orderer;
	private final RateChangeListener rateChangeListener;
	private final ConsumerConfig config;
	private List<LogConsumer<Type>> consumers = new ArrayList<>();
	private Map<LogId, Long> futureOwnedLogs;

	ConsumerImpl(Stream stream, LogClient logClient, Serializer<Type> deserializer, PositionImpl position,
			Orderer<Type> orderer, RateChangeListener rateChangeListener, ConsumerConfig config) {
		this.deserializer = deserializer;
		this.stream = stream;
		this.logClient = logClient;
		this.orderer = orderer;
		this.rateChangeListener = rateChangeListener;
		this.config = config;
		this.futureOwnedLogs = position.getFutureOwnedLogs();
		for (LogId l : position.getOwnedLogs()) {
			LogInputStream log = logClient.openLogForReading(l.getQualifiedName(), config.getLogConfig());
			log.setOffset(position.getOffsetForOwnedLog(l));
			consumers.add(new LogConsumerImpl<>(l, log, deserializer));
		}
	}

	@Override
	public Type getNextEvent(long timeout) {
		synchronized (consumers) {
			LogConsumer<Type> log = orderer.nextConsumer(consumers);
			try {
				return log.getNextEvent(timeout);
			} catch (EndOfLogException e) {
				handleEndOfLog(log);
				return null;
			}
		}
	}

	private void handleEndOfLog(LogConsumer<Type> oldLog) {
		consumers.remove(oldLog);
		LogId oldLogId = oldLog.getLogId();
		Optional<LogId> replacment = futureOwnedLogs.keySet().stream().filter(l -> l.succeeds(oldLogId)).findAny();
		if (replacment.isPresent()) {
			LogId logId = replacment.get();
			Long position = futureOwnedLogs.remove(logId);
			LogInputStream log = logClient.openLogForReading(logId.getQualifiedName(), config.getLogConfig());
			log.setOffset(position);
			consumers.add(new LogConsumerImpl<Type>(logId, log, deserializer));
			rateChangeListener.rateChanged(stream, false);
		} else {
			rateChangeListener.rateChanged(stream, true);
		}
	}

	@Override
	public Position getPosition() {
		synchronized (consumers) {
			Map<LogId, Long> positions = consumers.stream()
					.collect(Collectors.toMap(e -> e.getLogId(), e -> e.getOffset()));
			return new PositionImpl(positions, futureOwnedLogs);
		}
	}

	@Override
	public ConsumerConfig getConfig() {
		return config;
	}

	@Override
	public void setPosition(Position state) {
		synchronized (consumers) {
		// TODO Auto-generated method stub
		}
	}

}
