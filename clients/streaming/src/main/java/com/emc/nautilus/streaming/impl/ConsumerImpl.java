package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.emc.nautilus.logclient.EndOfSegmentException;
import com.emc.nautilus.logclient.LogServiceClient;
import com.emc.nautilus.logclient.SegmentInputStream;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.ConsumerConfig;
import com.emc.nautilus.streaming.Position;
import com.emc.nautilus.streaming.RateChangeListener;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;

public class ConsumerImpl<Type> implements Consumer<Type> {

    private final Serializer<Type> deserializer;
    private final LogServiceClient logServiceClient;

    private final Stream stream;
    private final Orderer<Type> orderer;
    private final RateChangeListener rateChangeListener;
    private final ConsumerConfig config;
    private final List<SegmentConsumer<Type>> consumers = new ArrayList<>();
    private final Map<SegmentId, Long> futureOwnedLogs;

    ConsumerImpl(Stream stream, LogServiceClient logClient, Serializer<Type> deserializer, PositionImpl position,
            Orderer<Type> orderer, RateChangeListener rateChangeListener, ConsumerConfig config) {
        this.deserializer = deserializer;
        this.stream = stream;
        this.logServiceClient = logClient;
        this.orderer = orderer;
        this.rateChangeListener = rateChangeListener;
        this.config = config;
        this.futureOwnedLogs = position.getFutureOwnedLogs();
        for (SegmentId s : position.getOwnedSegments()) {
            SegmentInputStream in = logClient.openLogForReading(s.getQualifiedName(), config.getSegmentConfig());
            in.setOffset(position.getOffsetForOwnedLog(s));
            consumers.add(new LogConsumerImpl<>(s, in, deserializer));
        }
    }

    @Override
    public Type getNextEvent(long timeout) {
        synchronized (consumers) {
            SegmentConsumer<Type> log = orderer.nextConsumer(consumers);
            try {
                return log.getNextEvent(timeout);
            } catch (EndOfSegmentException e) {
                handleEndOfLog(log);
                return null;
            }
        }
    }

    private void handleEndOfLog(SegmentConsumer<Type> oldSegment) {
        consumers.remove(oldSegment);
        SegmentId oldLogId = oldSegment.getLogId();
        Optional<SegmentId> replacment = futureOwnedLogs.keySet().stream().filter(l -> l.succeeds(oldLogId)).findAny();
        if (replacment.isPresent()) {
            SegmentId segmentId = replacment.get();
            Long position = futureOwnedLogs.remove(segmentId);
            SegmentInputStream in = logServiceClient.openLogForReading(segmentId.getQualifiedName(),
                                                                       config.getSegmentConfig());
            in.setOffset(position);
            consumers.add(new LogConsumerImpl<Type>(segmentId, in, deserializer));
            rateChangeListener.rateChanged(stream, false);
        } else {
            rateChangeListener.rateChanged(stream, true);
        }
    }

    @Override
    public Position getPosition() {
        synchronized (consumers) {
            Map<SegmentId, Long> positions = consumers.stream()
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
