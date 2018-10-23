/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EndOfSegmentException.ErrorType;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EventStreamReaderImpl<Type> implements EventStreamReader<Type> {

    private final Serializer<Type> deserializer;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory metadataClientFactory;

    private final Orderer orderer;
    private final ReaderConfig config;
    @GuardedBy("readers")
    private boolean closed;
    @GuardedBy("readers")
    private final List<EventSegmentReader> readers = new ArrayList<>();
    @GuardedBy("readers")
    private Sequence lastRead;
    @GuardedBy("readers")
    private String atCheckpoint;
    private final ReaderGroupStateManager groupState;
    private final Supplier<Long> clock;

    EventStreamReaderImpl(SegmentInputStreamFactory inputStreamFactory,
            SegmentMetadataClientFactory metadataClientFactory, Serializer<Type> deserializer,
            ReaderGroupStateManager groupState, Orderer orderer, Supplier<Long> clock, ReaderConfig config) {
        this.deserializer = deserializer;
        this.inputStreamFactory = inputStreamFactory;
        this.metadataClientFactory = metadataClientFactory;
        this.groupState = groupState;
        this.orderer = orderer;
        this.clock = clock;
        this.config = config;
        this.closed = false;
    }

    @Override
    public EventRead<Type> readNextEvent(long timeout) throws ReinitializationRequiredException, TruncatedDataException {
        synchronized (readers) {
            Preconditions.checkState(!closed, "Reader is closed");
            long waitTime = Math.min(timeout, ReaderGroupStateManager.TIME_UNIT.toMillis());
            Timer timer = new Timer();
            Segment segment = null;
            long offset = -1;
            ByteBuffer buffer;
            do { 
                String checkpoint = updateGroupStateIfNeeded();
                if (checkpoint != null) {
                     return createEmptyEvent(checkpoint);
                }
                EventSegmentReader segmentReader = orderer.nextSegment(readers);
                if (segmentReader == null) {
                    Exceptions.handleInterrupted(() -> Thread.sleep(waitTime));
                    buffer = null;
                } else {
                    segment = segmentReader.getSegmentId();
                    offset = segmentReader.getOffset();
                    try {
                        buffer = segmentReader.read(waitTime);
                    } catch (EndOfSegmentException e) {
                        boolean fetchSuccessors = e.getErrorType().equals(ErrorType.END_OF_SEGMENT_REACHED);
                        handleEndOfSegment(segmentReader, fetchSuccessors);
                        buffer = null;
                    } catch (SegmentTruncatedException e) {
                        handleSegmentTruncated(segmentReader);
                        buffer = null;
                    }
                }
            } while (buffer == null && timer.getElapsedMillis() < timeout);
            
            if (buffer == null) {
               return createEmptyEvent(null);
            } 
            lastRead = Sequence.create(segment.getSegmentId(), offset);
            int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
            return new EventReadImpl<>(lastRead,
                    deserializer.deserialize(buffer),
                    getPosition(),
                    new EventPointerImpl(segment, offset, length),
                    null);
        }
    }

    private EventRead<Type> createEmptyEvent(String checkpoint) {
        return new EventReadImpl<>(lastRead, null, getPosition(), null, checkpoint);
    }

    private PositionInternal getPosition() {
        Map<Segment, Long> positions = readers.stream()
                .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
        return new PositionImpl(positions);
    }

    /**
     * If the last call was a checkpoint updates the reader group state to indicate it has completed
     * and releases segments.
     * 
     * If a checkpoint is pending its identifier is returned. (The checkpoint will be considered
     * complete when this is invoked again.)
     * 
     * Otherwise it checks for any segments that need to be acquired.
     * 
     * Segments can only be released on the next read call following a checkpoint because this is
     * the only point we can be sure the caller has persisted their position, which is needed to be
     * sure the segment is located in the position of one of the readers and not left out because it
     * was moved while the checkpoint was occurring, while at the same time guaranteeing that
     * another reader will not see events following the ones read by this reader until after they
     * have been persisted.
     */
    @GuardedBy("readers")
    private String updateGroupStateIfNeeded() throws ReinitializationRequiredException {
        try {
            if (atCheckpoint != null) {
                groupState.checkpoint(atCheckpoint, getPosition());
                releaseSegmentsIfNeeded();
            }
            atCheckpoint = groupState.getCheckpoint();
            if (atCheckpoint != null) {
                log.info("{} at checkpoint {}", this, atCheckpoint);
                return atCheckpoint;
            }
            acquireSegmentsIfNeeded();
            return null;
        } catch (ReinitializationRequiredException e) {
            close();
            throw e;
        }
    }

    @GuardedBy("readers")
    private void releaseSegmentsIfNeeded() throws ReinitializationRequiredException {
        Segment segment = groupState.findSegmentToReleaseIfRequired();
        if (segment != null) {
            log.info("{} releasing segment {}", this, segment);
            EventSegmentReader reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            if (reader != null) {
                if (groupState.releaseSegment(segment, reader.getOffset(), getLag())) {
                    readers.remove(reader);
                    reader.close();
                }
            }
        }
    }

    @GuardedBy("readers")
    private void acquireSegmentsIfNeeded() throws ReinitializationRequiredException {
        Map<Segment, Long> newSegments = groupState.acquireNewSegmentsIfNeeded(getLag());
        if (!newSegments.isEmpty()) {
            log.info("{} acquiring segments {}", this, newSegments);
            for (Entry<Segment, Long> newSegment : newSegments.entrySet()) {
                final EventSegmentReader in = inputStreamFactory.createEventReaderForSegment(newSegment.getKey(),
                        groupState.getEndOffsetForSegment(newSegment.getKey()));
                in.setOffset(newSegment.getValue());
                readers.add(in);
            }
        }
    }

    //TODO: This is broken until https://github.com/pravega/pravega/issues/191 is implemented.
    private long getLag() {
        if (lastRead == null) {
            return 0;
        }
        return clock.get() - lastRead.getHighOrder();
    }
    
    private void handleEndOfSegment(EventSegmentReader oldSegment, boolean fetchSuccessors) throws ReinitializationRequiredException {
        try {
            log.info("{} encountered end of segment {} ", this, oldSegment.getSegmentId());
            readers.remove(oldSegment);
            oldSegment.close();
            groupState.handleEndOfSegment(oldSegment.getSegmentId(), fetchSuccessors);
        } catch (ReinitializationRequiredException e) {
            close();
            throw e;
        }
    }
    
    private void handleSegmentTruncated(EventSegmentReader segmentReader) throws ReinitializationRequiredException, TruncatedDataException {
        Segment segmentId = segmentReader.getSegmentId();
        log.info("{} encountered truncation for segment {} ", this, segmentId);
        String delegationToken = groupState.getOrRefreshDelegationTokenFor(segmentId);

        @Cleanup
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(segmentId, delegationToken);
        try {
            long startingOffset = metadataClient.getSegmentInfo().getStartingOffset();
            segmentReader.setOffset(startingOffset);
        } catch (NoSuchSegmentException e) {
            handleEndOfSegment(segmentReader, true);
        }
        throw new TruncatedDataException();
    }

    @Override
    public ReaderConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        synchronized (readers) {
            if (!closed) {
                log.info("Closing reader {} ", this);
                closed = true;
                groupState.readerShutdown(getPosition());
                for (EventSegmentReader reader : readers) {
                    reader.close();
                }
                readers.clear();
                groupState.close();
            }
        }
    }

    @Override
    public Type fetchEvent(EventPointer pointer) throws NoSuchEventException {
        Preconditions.checkNotNull(pointer);
        // Create SegmentInputStream
        @Cleanup
        EventSegmentReader inputStream = inputStreamFactory.createEventReaderForSegment(pointer.asImpl().getSegment(),
                                                                                        pointer.asImpl().getEventLength());
        inputStream.setOffset(pointer.asImpl().getEventStartOffset());
        // Read event
        try {
            ByteBuffer buffer = inputStream.read();
            Type result = deserializer.deserialize(buffer);
            return result;
        } catch (EndOfSegmentException e) {
            throw new NoSuchEventException(e.getMessage());
        } catch (NoSuchSegmentException | SegmentTruncatedException e) {
            throw new NoSuchEventException("Event no longer exists.");
        }
    }

    @Synchronized
    @VisibleForTesting
    List<EventSegmentReader> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    @Override
    public String toString() {
        return "EventStreamReaderImpl( id=" + groupState.getReaderId() + ")";
    }
    
}
