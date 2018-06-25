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
import io.pravega.client.segment.impl.AsyncSegmentEventReader;
import io.pravega.client.segment.impl.AsyncSegmentEventReaderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EndOfSegmentException.ErrorType;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStreamReaderImpl<Type> implements EventStreamReader<Type> {

    static final long UNBOUNDED_END_OFFSET = Long.MAX_VALUE;

    private final Serializer<Type> deserializer;
    private final AsyncSegmentEventReaderFactory readerFactory;
    private final SegmentMetadataClientFactory metadataClientFactory;

    private final ReaderConfig config;
    @GuardedBy("readers")
    private boolean closed;
    @GuardedBy("readers")
    private final List<ReaderState> readers = new ArrayList<>();
    private final BlockingQueue<ReaderState> readCompletionQueue;
    @GuardedBy("readers")
    private Sequence lastRead;
    @GuardedBy("readers")
    private boolean atCheckpoint;
    private final ReaderGroupStateManager groupState;
    private final Supplier<Long> clock;

    EventStreamReaderImpl(AsyncSegmentEventReaderFactory readerFactory,
            SegmentMetadataClientFactory metadataClientFactory, Serializer<Type> deserializer,
            ReaderGroupStateManager groupState, Supplier<Long> clock, ReaderConfig config,
            BlockingQueue<ReaderState> readCompletionQueue) {
        this.deserializer = deserializer;
        this.readerFactory = readerFactory;
        this.metadataClientFactory = metadataClientFactory;
        this.groupState = groupState;
        this.clock = clock;
        this.config = config;
        this.closed = false;
        this.readCompletionQueue = readCompletionQueue;
    }

    @Override
    @SneakyThrows
    public EventRead<Type> readNextEvent(long timeout) throws ReinitializationRequiredException, TruncatedDataException {
        long traceId = LoggerHelpers.traceEnter(log, "readNextEvent");
        try {
            synchronized (readers) {
                Preconditions.checkState(!closed, "Reader is closed");
                Timer timer = new Timer();
                Segment segment = null;
                long offset = -1;
                ByteBuffer buffer = null;
                do {
                    String checkpoint = updateGroupStateIfNeeded();
                    if (checkpoint != null) {
                        return createEmptyEvent(checkpoint);
                    }

                    // poll for a completed read
                    long waitTime = Math.max(0, Math.min(timeout - timer.getElapsedMillis(), ReaderGroupStateManager.TIME_UNIT.toMillis()));
                    ReaderState segmentReader = Exceptions.handleInterrupted(() -> readCompletionQueue.poll(waitTime, TimeUnit.MILLISECONDS));

                    // note: a read may complete with a value and be enqueued for processing, but its reader be released
                    // and closed before the value has been processed.  In this scenario we discard the value.
                    if (segmentReader != null && !segmentReader.isClosed()) {
                        try {
                            segment = segmentReader.getSegmentId();
                            offset = segmentReader.getReadOffset();
                            buffer = segmentReader.takeEvent();
                        } catch (Throwable th) {
                            th = Exceptions.unwrap(th);
                            if (th instanceof EndOfSegmentException) {
                                EndOfSegmentException e = (EndOfSegmentException) th;
                                assert e.getErrorType().equals(ErrorType.END_OF_SEGMENT_REACHED);
                                boolean fetchSuccessors = e.getErrorType().equals(ErrorType.END_OF_SEGMENT_REACHED);
                                handleEndOfSegment(segmentReader, fetchSuccessors);
                            } else if (th instanceof SegmentTruncatedException) {
                                handleSegmentTruncated(segmentReader);
                            } else {
                                throw th;
                            }
                        } finally {
                            // schedule the next read if the reader is still open.
                            if (!segmentReader.isClosed()) {
                                if (!segmentReader.hasNext()) {
                                    // the configured end offset was reached; note that the buffer still contains a valid event
                                    handleEndOfSegment(segmentReader, false);
                                } else {
                                    segmentReader.readNext(readCompletionQueue);
                                }
                            }
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
        } finally {
            LoggerHelpers.traceLeave(log, "readNextEvent", traceId);
        }
    }

    private EventRead<Type> createEmptyEvent(String checkpoint) {
        return new EventReadImpl<>(lastRead, null, getPosition(), null, checkpoint);
    }

    private PositionInternal getPosition() {
        Map<Segment, Long> positions = readers.stream()
                .collect(Collectors.toMap(ReaderState::getSegmentId, ReaderState::getReadOffset));
        return new PositionImpl(positions);
    }
    
    /**
     * Releases or acquires segments as needed. Returns the name of the checkpoint if the reader is
     * at one.
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
            String checkpoint = groupState.getCheckpoint();
            if (checkpoint == null) {
                if (atCheckpoint) {
                    releaseSegmentsIfNeeded();
                    atCheckpoint = false;
                }
                acquireSegmentsIfNeeded();
                return null;
            } else {
                log.info("{} at checkpoint {}", this, checkpoint);
                groupState.checkpoint(checkpoint, getPosition());
                atCheckpoint = true;
                return checkpoint;
            }
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
            ReaderState reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            if (reader != null) {
                if (groupState.releaseSegment(segment, reader.getReadOffset(), getLag())) {
                    readers.remove(reader);

                    // note that the read completion handler may have already enqueued the reader
                    // and so readNextEvent should avoid using the value of closed readers.
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
                AsyncSegmentEventReader r = readerFactory.createEventReaderForSegment(
                        newSegment.getKey(),
                        AsyncSegmentEventReaderFactory.DEFAULT_BUFFER_SIZE);
                long startOffset = newSegment.getValue();
                long endOffset = groupState.getEndOffsetForSegment(newSegment.getKey());
                if (endOffset <= startOffset) {
                    throw new IllegalStateException("acquired a reader where end <= start");
                }
                ReaderState reader = new ReaderState(r, startOffset, endOffset);
                readers.add(reader);
                reader.readNext(readCompletionQueue);
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
    
    private void handleEndOfSegment(ReaderState oldSegment, boolean fetchSuccessors) throws ReinitializationRequiredException {
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
    
    private void handleSegmentTruncated(ReaderState segmentReader) throws ReinitializationRequiredException, TruncatedDataException {
        Segment segmentId = segmentReader.getSegmentId();
        log.info("{} encountered truncation for segment {} ", this, segmentId);
        String delegationToken = groupState.getLatestDelegationToken();
        @Cleanup
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(segmentId, delegationToken);
        try {
            long startingOffset = metadataClient.getSegmentInfo().getStartingOffset();
            segmentReader.setReadOffset(startingOffset);
            log.info("{} fast-forwarded to offset {} for segment {}", this, startingOffset, segmentId);
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
                for (ReaderState reader : readers) {
                    reader.close();
                }
                readers.clear();
                groupState.close();
            }
        }
    }

    @Override
    @SneakyThrows
    public Type fetchEvent(EventPointer pointer) throws NoSuchEventException {
        Preconditions.checkNotNull(pointer);
        // Create AsyncSegmentEventReader
        @Cleanup
        AsyncSegmentEventReader reader = readerFactory.createEventReaderForSegment(
                pointer.asImpl().getSegment(),
                pointer.asImpl().getEventLength());

        // Read event
        try {
            ByteBuffer buffer = reader.readAsync(pointer.asImpl().getEventStartOffset()).join();
            Type result = deserializer.deserialize(buffer);
            return result;
        } catch (Exception e) {
            Throwable th = Exceptions.unwrap(e);
            if (th instanceof EndOfSegmentException) {
                throw new NoSuchEventException(e.getMessage());
            } else if (th instanceof NoSuchSegmentException || th instanceof SegmentTruncatedException) {
                throw new NoSuchEventException("Event no longer exists.");
            }
            throw th;
        }
    }

    @Synchronized
    @VisibleForTesting
    List<ReaderState> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    @Synchronized
    @VisibleForTesting
    BlockingQueue<ReaderState> getQueue() {
        return readCompletionQueue;
    }

    @Override
    public String toString() {
        return "EventStreamReaderImpl( id=" + groupState.getReaderId() + ")";
    }

    @ToString
    static class ReaderState {
        @VisibleForTesting
        final AsyncSegmentEventReader reader;
        @VisibleForTesting
        CompletableFuture<ByteBuffer> outstandingRead = CompletableFuture.completedFuture(null);
        private final long endOffset;
        private long readOffset;

        public ReaderState(AsyncSegmentEventReader reader, long readOffset, long endOffset) {
            this.reader = reader;
            this.readOffset = readOffset;
            this.endOffset = endOffset;
        }

        public Segment getSegmentId() {
            return reader.getSegmentId();
        }

        /**
         * Gets the offset up to which events have been processed for this reader.
         * This offset corresponds to the checkpoint position, in contrast to the position
         * within {@code reader} which advances asynchronously as completed reads are queued.
         */
        public long getReadOffset() {
            return readOffset;
        }

        /**
         * Sets the offset up to which events have been processed for this reader.
         *
         * @param offset the offset.
         */
        public void setReadOffset(long offset) {
            readOffset = offset;
        }

        /**
         * Gets the end offset (exclusive) for this reader.
         */
        long getEndOffset() {
            return endOffset;
        }

        /**
         * Takes the read event and updates the read offset accordingly.
         *
         * @throws CompletionException if the outstanding read completed exceptionally
         */
        public ByteBuffer takeEvent() {
            assert outstandingRead.isDone();
            ByteBuffer buffer = outstandingRead.getNow(null);
            int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
            readOffset += length;
            return buffer;
        }

        /**
         * Indicates whether to read more events (or the configured end offset has been reached).
         */
        public boolean hasNext() {
            return endOffset == UNBOUNDED_END_OFFSET || readOffset < endOffset;
        }

        /**
         * Reads the next event.
         *
         * @param queue the queue for completed reads.
         */
        public void readNext(final Queue<ReaderState> queue) {
            assert outstandingRead.isDone();
            outstandingRead = reader.readAsync(readOffset);
            outstandingRead.whenComplete((buf, th) -> {
                // note: don't enqueue cancelled reads since the reader is closing anyway.
                if (!(th instanceof CancellationException)) {
                    queue.add(this);
                }
            });
        }

        public void close() {
            outstandingRead.cancel(false);
            reader.close();
        }

        public boolean isClosed() {
            return reader.isClosed();
        }
    }
}
