/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import com.google.common.collect.ImmutableMap;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
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
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderNotInReaderGroupException;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.SegmentWithRange.Range;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.segment.impl.EndOfSegmentException.ErrorType.END_OF_SEGMENT_REACHED;

@Slf4j
public class EventStreamReaderImpl<Type> implements EventStreamReader<Type> {

    // Base waiting time for a reader on an idle segment waiting for new data to be read.
    private static final long BASE_READER_WAITING_TIME_MS = ReaderGroupStateManager.TIME_UNIT.toMillis();

    private final Serializer<Type> deserializer;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory metadataClientFactory;

    private final Orderer orderer;
    private final ReaderConfig config;
   
    private final ImmutableMap<Stream, WatermarkReaderImpl> waterMarkReaders;
    @GuardedBy("readers")
    private boolean closed;
    @GuardedBy("readers")
    private final List<EventSegmentReader> readers = new ArrayList<>();
    @GuardedBy("readers")
    private final Map<Segment, Range> ranges = new HashMap<>();
    @GuardedBy("readers")
    private final Map<Segment, Long> sealedSegments = new HashMap<>();
    @GuardedBy("readers")
    private Sequence lastRead;
    @GuardedBy("readers")
    private String atCheckpoint;
    private final ReaderGroupStateManager groupState;
    private final Supplier<Long> clock;
    private final Controller controller;
    private final Semaphore segmentsWithData;

    EventStreamReaderImpl(SegmentInputStreamFactory inputStreamFactory,
            SegmentMetadataClientFactory metadataClientFactory, Serializer<Type> deserializer,
            ReaderGroupStateManager groupState, Orderer orderer, Supplier<Long> clock, ReaderConfig config, 
            ImmutableMap<Stream, WatermarkReaderImpl> waterMarkReaders, Controller controller) {
        this.deserializer = deserializer;
        this.inputStreamFactory = inputStreamFactory;
        this.metadataClientFactory = metadataClientFactory;
        this.groupState = groupState;
        this.orderer = orderer;
        this.clock = clock;
        this.config = config;
        this.waterMarkReaders = waterMarkReaders;
        this.closed = false;
        this.controller = controller;
        this.segmentsWithData = new Semaphore(0);
    }

    @Override
    public EventRead<Type> readNextEvent(long timeoutMillis) throws ReinitializationRequiredException, TruncatedDataException {
        synchronized (readers) {
            Preconditions.checkState(!closed, "Reader is closed");
            try {
                return readNextEventInternal(timeoutMillis);
            } catch (ReaderNotInReaderGroupException e) {
                close();
                throw new ReinitializationRequiredException(e);
            }
        }
    }
    
    private EventRead<Type> readNextEventInternal(long timeoutMillis) throws ReaderNotInReaderGroupException, TruncatedDataException {
        long firstByteTimeoutMillis = Math.min(timeoutMillis, BASE_READER_WAITING_TIME_MS);
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
                blockFor(firstByteTimeoutMillis);
                segmentsWithData.drainPermits();
                buffer = null;
            } else {
                segment = segmentReader.getSegmentId();
                offset = segmentReader.getOffset();
                try {
                    buffer = segmentReader.read(firstByteTimeoutMillis);
                } catch (EndOfSegmentException e) {
                    boolean isSegmentSealed = e.getErrorType().equals(END_OF_SEGMENT_REACHED);
                    handleEndOfSegment(segmentReader, isSegmentSealed);
                    buffer = null;
                } catch (SegmentTruncatedException e) {
                    handleSegmentTruncated(segmentReader);
                    buffer = null;
                }
            }
        } while (buffer == null && timer.getElapsedMillis() < timeoutMillis);

        if (buffer == null) {
            log.debug("Empty event returned for reader {} ", groupState.getReaderId());
            return createEmptyEvent(null);
        } 
        lastRead = Sequence.create(segment.getSegmentId(), offset);
        int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
        return new EventReadImpl<>(deserializer.deserialize(buffer), getPosition(),
                                   new EventPointerImpl(segment, offset, length), null);
    }

    private void blockFor(long timeoutMs) {
        Exceptions.handleInterrupted(() -> {
            @SuppressWarnings("unused")
            boolean acquired = segmentsWithData.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        });
    }
    
    private EventRead<Type> createEmptyEvent(String checkpoint) {
        return new EventReadImpl<>(null, getPosition(), null, checkpoint);
    }

    private PositionInternal getPosition() {
        Map<Segment, Long> ownedSegments = new HashMap<>(sealedSegments);
        for (EventSegmentReader entry : readers) {
            ownedSegments.put(entry.getSegmentId(), entry.getOffset());
        }
        return PositionImpl.builder().ownedSegments(ownedSegments).segmentRanges(new HashMap<>(ranges)).build();
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
    private String updateGroupStateIfNeeded() throws ReaderNotInReaderGroupException {
        PositionInternal position = getPosition();
        if (atCheckpoint != null) {
            groupState.checkpoint(atCheckpoint, position);
            log.info("Reader {} completed checkpoint {}", groupState.getReaderId(), atCheckpoint);
            releaseSegmentsIfNeeded(position);
        }
        String checkpoint = groupState.getCheckpoint();
        while (checkpoint != null) {
            log.info("{} at checkpoint {}", this, checkpoint);
            if (groupState.isCheckpointSilent(checkpoint)) {
                // Checkpoint the reader immediately with the current position. Checkpoint Event is not generated.
                groupState.checkpoint(checkpoint, position);
                if (atCheckpoint != null) {
                    //In case the silent checkpoint held up releasing segments
                    releaseSegmentsIfNeeded(position);
                    atCheckpoint = null;
                }
                checkpoint = groupState.getCheckpoint();
            } else {
                atCheckpoint = checkpoint;
                return atCheckpoint;
            }
        }
        atCheckpoint = null;
        if (acquireSegmentsIfNeeded(position) || groupState.updateLagIfNeeded(getLag(), position)) {
            waterMarkReaders.forEach((stream, reader) -> {
                reader.advanceTo(groupState.getLastReadpositions(stream));
            });
        }
        return null;
    }

    /**
     * Releases segments. This must not be invoked except immediately after a checkpoint.
     */
    @GuardedBy("readers")
    private void releaseSegmentsIfNeeded(PositionInternal position) throws ReaderNotInReaderGroupException {
        releaseSealedSegments();
        Segment segment = groupState.findSegmentToReleaseIfRequired();
        if (segment != null) {
            log.info("{} releasing segment {}", this, segment);
            EventSegmentReader reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            if (reader != null) {
                if (groupState.releaseSegment(segment, reader.getOffset(), getLag(), position)) {
                    readers.remove(reader);
                    ranges.remove(reader.getSegmentId());
                    reader.close();
                }
            }
        }
    }
    
    /**
     * Releases all sealed segments, unless there is a checkpoint pending for this reader.
     */
    private void releaseSealedSegments() throws ReaderNotInReaderGroupException {
        for (Iterator<Entry<Segment, Long>> iterator = sealedSegments.entrySet().iterator(); iterator.hasNext();) {
            Segment oldSegment = iterator.next().getKey();
            Range range = ranges.get(oldSegment);
            if (groupState.handleEndOfSegment(new SegmentWithRange(oldSegment, range))) {
                ranges.remove(oldSegment);
                iterator.remove();
            } else {
                break;
            }
        }    
    }

    @GuardedBy("readers")
    private boolean acquireSegmentsIfNeeded(PositionInternal position) throws ReaderNotInReaderGroupException {
        Map<SegmentWithRange, Long> newSegments = groupState.acquireNewSegmentsIfNeeded(getLag(), position);
        if (!newSegments.isEmpty()) {
            log.info("{} acquiring segments {}", this, newSegments);
            for (Entry<SegmentWithRange, Long> newSegment : newSegments.entrySet()) {
                long endOffset = groupState.getEndOffsetForSegment(newSegment.getKey().getSegment());
                if (newSegment.getValue() < 0 || (newSegment.getValue() == endOffset && endOffset != Long.MAX_VALUE)) {
                    sealedSegments.put(newSegment.getKey().getSegment(), newSegment.getValue());
                    ranges.put(newSegment.getKey().getSegment(), newSegment.getKey().getRange());
                } else {
                    Segment segment = newSegment.getKey().getSegment();
                    EventSegmentReader in = inputStreamFactory.createEventReaderForSegment(segment, config.getBufferSize(),
                                                                                           segmentsWithData, endOffset);
                    in.setOffset(newSegment.getValue());
                    readers.add(in);
                    ranges.put(segment, newSegment.getKey().getRange());
                }
            }
            segmentsWithData.release();
            return true;
        }
        return false;
    }

    //TODO: This is broken until https://github.com/pravega/pravega/issues/191 is implemented.
    private long getLag() {
        if (lastRead == null) {
            return 0;
        }
        return clock.get() - lastRead.getHighOrder();
    }
    
    @GuardedBy("readers")
    private void handleEndOfSegment(EventSegmentReader oldSegment, boolean segmentSealed) {
            Segment segmentId = oldSegment.getSegmentId();
        log.info("{} encountered end of segment {} ", this, oldSegment.getSegmentId());
        readers.remove(oldSegment);
        oldSegment.close();
        sealedSegments.put(segmentId, segmentSealed ? -1L : oldSegment.getOffset());
    }
    
    private void handleSegmentTruncated(EventSegmentReader segmentReader) throws TruncatedDataException {
        Segment segmentId = segmentReader.getSegmentId();
        log.info("{} encountered truncation for segment {} ", this, segmentId);

        @Cleanup
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(segmentId,
                DelegationTokenProviderFactory.create(controller, segmentId));
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
        closeAt(getPosition());
        for (WatermarkReaderImpl reader : waterMarkReaders.values()) {
            reader.close();
        }           
    }

    @Override
    public void closeAt(Position position) {
        synchronized (readers) {
            if (!closed) {
                log.info("Closing reader {} at position {}.", this, position);
                closed = true;
                groupState.readerShutdown(position);
                for (EventSegmentReader reader : readers) {
                    reader.close();
                }
                readers.clear();
                ranges.clear();
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

    @VisibleForTesting
    List<EventSegmentReader> getReaders() {
        synchronized (readers) {            
            return Collections.unmodifiableList(readers);
        }
    }
    
    @VisibleForTesting
    Map<Segment, Range> getRanges() {
        synchronized (readers) {
            return Collections.unmodifiableMap(ranges);
        }
    }

    @Override
    public String toString() {
        return "EventStreamReaderImpl( id=" + groupState.getReaderId() + ")";
    }

    @Override
    public TimeWindow getCurrentTimeWindow(Stream stream) {
        if (getConfig().isDisableTimeWindows()) {
            return new TimeWindow(null, null);
        }
        WatermarkReaderImpl tracker = waterMarkReaders.get(stream);
        if (tracker == null) {
            throw new IllegalArgumentException("Reader is not subscribed to stream: " + stream);
        } else {
            return tracker.getTimeWindow();
        }
    }
    
}
