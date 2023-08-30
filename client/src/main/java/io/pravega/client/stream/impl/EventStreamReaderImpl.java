/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.control.impl.Controller;
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
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.common.util.CopyOnWriteHashMap;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.segment.impl.EndOfSegmentException.ErrorType.END_OF_SEGMENT_REACHED;

@Slf4j
public final class EventStreamReaderImpl<Type> implements EventStreamReader<Type> {

    // Base waiting time for a reader on an idle segment waiting for new data to be read.
    private static final long BASE_READER_WAITING_TIME_MS = ReaderGroupStateManager.TIME_UNIT.toMillis();
    // As an optimization to avoid creating a new ownedSegments map per event read, we define a base map of segments and
    // then a batch of updates to the offsets of these segments, one per event read. Internally, the Position object can
    // derive the right offsets at which the event was read by lazily replying such updates up to the point it was read.
    private static final int MAX_BUFFERED_SEGMENT_OFFSET_UPDATES = 1000;

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
    private final Map<Segment, Long> sealedSegments = new HashMap<>();
    @GuardedBy("readers")
    private Sequence lastRead;
    // Ranges, ownedSegments and segmentOffsetUpdates may be lazily accessed by PositionImpl objects to build their
    // state. The objective is to avoid creating per-event collections for performance reasons.
    private CopyOnWriteHashMap<Segment, Range> ranges = new CopyOnWriteHashMap<>();
    private Map<Segment, Long> ownedSegments = new HashMap<>();
    private List<Entry<Segment, Long>> segmentOffsetUpdates = newImmutableSegmentOffsetUpdatesList();
    @GuardedBy("readers")
    private int segmentOffsetUpdatesIndex = 0;
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
        ByteBuffer buffer = null;
        do {
            String checkpoint = updateGroupStateIfNeeded();
            if (checkpoint != null) {
                // return checkpoint event to user
                return createEmptyEvent(checkpoint);
            }

            EventSegmentReader segmentReader = orderer.nextSegment(readers);
            if (segmentReader == null) {
                log.debug("Reader {} has no segment to read. Number of available segments = {}.", groupState.getReaderId(), readers.size());
                if (groupState.reachedEndOfStream()) {
                    log.info("Empty event returned for reader {} as it reached end of stream ", groupState.getReaderId());
                    return createEmptyEventEndOfStream();
                }
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
                    handleSegmentTruncated(segmentReader, timeoutMillis);
                    buffer = null;
                } finally {
                    if (buffer == null) {
                        refreshAndGetPosition();
                    }
                }
            }
        } while (buffer == null && timer.getElapsedMillis() < timeoutMillis);

        if (buffer == null) {
            log.debug("Empty event returned for reader {} ", groupState.getReaderId());
            return createEmptyEvent(null);
        } 
        lastRead = Sequence.create(segment.getSegmentId(), offset);
        int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
        addSegmentOffsetUpdateIfNeeded(segment, offset + length);
        return new EventReadImpl<>(deserializer.deserialize(buffer), getCurrentPosition(), new EventPointerImpl(segment, offset, length), null);
    }

    private void addSegmentOffsetUpdateIfNeeded(Segment segment, long offset) {
        if (segmentOffsetUpdatesIndex >= MAX_BUFFERED_SEGMENT_OFFSET_UPDATES) {
            refreshAndGetPosition();
        } else {
            segmentOffsetUpdates.set(segmentOffsetUpdatesIndex, new SimpleEntry<>(segment, offset));
            segmentOffsetUpdatesIndex++;
        }
    }

    private void blockFor(long timeoutMs) {
        Exceptions.handleInterrupted(() -> {
            @SuppressWarnings("unused")
            boolean acquired = segmentsWithData.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
        });
    }
    
    private EventRead<Type> createEmptyEvent(String checkpoint) {
        return new EventReadImpl<>(null, refreshAndGetPosition(), null, checkpoint);
    }

    private EventRead<Type> createEmptyEventEndOfStream() {
        return new EventReadImpl<>(null, refreshAndGetPosition(), null, null, true);
    }

    /**
     * Updates the cached ownedSegments and segmentOffsetUpdates for this reader. This should be executed every time
     * there is a change in the Segments being managed by a reader in order to build correct Position objects.
     *
     * @return New position object with the most recent ownedSegments and segmentOffsetUpdates state.
     */
    private PositionInternal refreshAndGetPosition() {
        // We need to create new objects for segmentOffsetUpdates and ownedSegments, as there could be Position objects
        // pointing to the current state of existing segmentOffsetUpdates and ownedSegments to build their internal state.
        segmentOffsetUpdates = newImmutableSegmentOffsetUpdatesList();
        segmentOffsetUpdatesIndex = 0;
        ownedSegments = new HashMap<>(sealedSegments);
        for (EventSegmentReader entry : readers) {
            ownedSegments.put(entry.getSegmentId(), entry.getOffset());
        }
        return getCurrentPosition();
    }

    /**
     * Creates a new {@link PositionInternal} object based on the current state of the reader. The object is created
     * using a builder that enables the lazy computation of its internal state.
     *
     * @return New {@link PositionInternal} object with current state of the reader.
     */
    private PositionInternal getCurrentPosition() {
        return PositionImpl.builder().ownedSegments(ownedSegments)
                                     .segmentRanges(ranges.getInnerMap())
                                     .updatesToSegmentOffsets(segmentOffsetUpdates.subList(0, segmentOffsetUpdatesIndex))
                                     .build();
    }

    /**
     * As a segmentOffsetUpdates object may be read by multiple PositionImpl objects, we need to ensure that that list
     * is structurally immutable (i.e., cannot be resized), but it should allow setting and reading values for list
     * positions. Any attempt to add/remove elements from the list results in {@link java.lang.UnsupportedOperationException}.
     *
     * @return New immutable array of Entry<Segment, Long> wrapped by a List interface.
     */
    @SuppressWarnings("unchecked")
    private List<Entry<Segment, Long>> newImmutableSegmentOffsetUpdatesList() {
        return Arrays.asList((Entry<Segment, Long>[]) new Entry[MAX_BUFFERED_SEGMENT_OFFSET_UPDATES]);
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
        groupState.updateConfigIfNeeded();
        PositionInternal position = null;
        if (atCheckpoint != null) {
            // process the checkpoint we're at
            position = refreshAndGetPosition();
            groupState.checkpoint(atCheckpoint, position);
            log.info("Reader {} completed checkpoint {} at postion {}", groupState.getReaderId(), atCheckpoint, position);
            releaseSegmentsIfNeeded(position);
            groupState.updateTruncationStreamCutIfNeeded();
        }
        String checkpoint = groupState.getCheckpoint();
        while (checkpoint != null) {
            log.info("{} at checkpoint {}", this, checkpoint);
            if (groupState.isCheckpointSilent(checkpoint)) {
                position = refreshAndGetPosition();
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

        if (position != null || lastRead == null || groupState.canAcquireSegmentIfNeeded() || groupState.canUpdateLagIfNeeded()) {
            position = (position == null) ? refreshAndGetPosition() : position;
            if (acquireSegmentsIfNeeded(position) || groupState.updateLagIfNeeded(getLag(), position)) {
                waterMarkReaders.forEach((stream, reader) -> reader.advanceTo(groupState.getLastReadpositions(stream)));
                refreshAndGetPosition();
            }
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
            log.info("{} releasing sealed segment {}", this, oldSegment);
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
    
    private void handleSegmentTruncated(EventSegmentReader segmentReader, long timeoutInMilli) throws TruncatedDataException {
        Segment segmentId = segmentReader.getSegmentId();
        log.info("{} encountered truncation for segment {} ", this, segmentId);

        @Cleanup
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(segmentId,
                DelegationTokenProviderFactory.create(controller, segmentId, AccessOperation.READ));
        try {
            long startingOffset = Futures.getThrowingExceptionWithTimeout(metadataClient.getSegmentInfo(), timeoutInMilli).getStartingOffset();
            if (segmentReader.getOffset() == startingOffset) {
                log.warn("Attempt to fetch the next available read offset on the segment {} returned a truncated offset {}",
                         segmentId, startingOffset);
            }
            segmentReader.setOffset(startingOffset);
        } catch (NoSuchSegmentException e) {
            handleEndOfSegment(segmentReader, true);
        } catch (TimeoutException te) {
            log.warn("A timeout has occurred while attempting to retrieve segment information from the server");
        }
        throw new TruncatedDataException();
    }

    @Override
    public ReaderConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        closeAt(refreshAndGetPosition());
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
                ranges = new CopyOnWriteHashMap<>();
                ownedSegments = new HashMap<>();
                segmentOffsetUpdates = newImmutableSegmentOffsetUpdatesList();
                segmentOffsetUpdatesIndex = 0;
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
            return deserializer.deserialize(buffer);
        } catch (EndOfSegmentException e) {
            throw new NoSuchEventException(e.getMessage());
        } catch (NoSuchSegmentException | SegmentTruncatedException e) {
            throw new NoSuchEventException("Event no longer exists.");
        }
    }

    @VisibleForTesting
    List<EventSegmentReader> getReaders() {
        synchronized (readers) {            
            return ImmutableList.copyOf(readers);
        }
    }

    @VisibleForTesting
    Map<Segment, Range> getRanges() {
        synchronized (readers) {
            return ImmutableMap.copyOf(ranges.getInnerMap());
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