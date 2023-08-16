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
import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.Update;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.SegmentWithRange.Range;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataInput.ElementDeserializer;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.RevisionDataOutput.ElementSerializer;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

/**
 * This class encapsulates the state machine of a reader group. The class represents the full state,
 * and each of the nested classes are state transitions that can occur.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class ReaderGroupState implements Revisioned {

    private static final long ASSUMED_LAG_MILLIS = 30000;
    private final String scopedSynchronizerStream;
    @Getter
    private final ReaderGroupConfig config;
    @GuardedBy("$lock")
    private Revision revision;
    @GuardedBy("$lock")
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final CheckpointState checkpointState;
    @GuardedBy("$lock")
    private final Map<String, Long> distanceToTail;
    @GuardedBy("$lock")
    private final Map<SegmentWithRange, Set<Long>> futureSegments;
    @GuardedBy("$lock")
    private final Map<String, Map<SegmentWithRange, Long>> assignedSegments;
    @GuardedBy("$lock")
    private final Map<SegmentWithRange, Long> unassignedSegments;
    @GuardedBy("$lock")
    private final Map<SegmentWithRange, Long> lastReadPosition;
    private final Map<Segment, Long> endSegments;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private boolean updatingConfig;
    
    ReaderGroupState(String scopedSynchronizerStream, Revision revision, ReaderGroupConfig config, Map<SegmentWithRange, Long> segmentsToOffsets,
                     Map<Segment, Long> endSegments, boolean updatingConfig) {
        Exceptions.checkNotNullOrEmpty(scopedSynchronizerStream, "scopedSynchronizerStream");
        Preconditions.checkNotNull(revision);
        Preconditions.checkNotNull(config);
        Exceptions.checkNotNullOrEmpty(segmentsToOffsets.entrySet(), "segmentsToOffsets");
        this.scopedSynchronizerStream = scopedSynchronizerStream;
        this.config = config;
        this.revision = revision;
        this.checkpointState = new CheckpointState();
        this.distanceToTail = new HashMap<>();
        this.futureSegments = new HashMap<>();
        this.assignedSegments = new HashMap<>();
        this.unassignedSegments = new LinkedHashMap<>(segmentsToOffsets);
        this.lastReadPosition = new HashMap<>(segmentsToOffsets);
        this.endSegments = ImmutableMap.copyOf(endSegments);
        this.updatingConfig = updatingConfig;
    }

    /**
     * @return A map from Reader to a relative measure of how much data they have to process. The
     *         scale is calibrated to where 1.0 is equal to the largest segment.
     */
    @Synchronized
    Map<String, Double> getRelativeSizes() {
        long maxDistance = Long.MIN_VALUE;
        Map<String, Double> result = new HashMap<>();
        for (Entry<String, Long> entry : distanceToTail.entrySet()) {
            Map<SegmentWithRange, Long> segments = assignedSegments.get(entry.getKey());
            if (segments != null && !segments.isEmpty()) {
                maxDistance = Math.max(Math.max(ASSUMED_LAG_MILLIS, entry.getValue()), maxDistance);
            }
        }
        for (Entry<String, Map<SegmentWithRange, Long>> entry : assignedSegments.entrySet()) {
            if (entry.getValue().isEmpty()) {
                result.put(entry.getKey(), 0.0);
            } else {
                long distance = Math.max(ASSUMED_LAG_MILLIS, distanceToTail.get(entry.getKey()));
                result.put(entry.getKey(), entry.getValue().size() * distance / (double) maxDistance);
            }
        }
        return result;
    }
    
    @Synchronized
    int getNumberOfReaders() {
        return assignedSegments.size();
    }
    
    @Synchronized
    public Set<String> getOnlineReaders() {
        return new HashSet<>(assignedSegments.keySet());
    }
    
    /**
     * @return The 0 indexed ranking of the requested reader in the reader group in terms of amount
     *         of keyspace assigned to it, or -1 if the reader is not part of the group.
     *         The reader with the most keyspace will be 0 and the reader with the least keyspace will be numReaders-1.
     */
    @Synchronized
    int getRanking(String reader) {
        List<String> sorted = getRelativeSizes().entrySet()
                                   .stream()
                                   .sorted((o1, o2) -> Double.compare(o2.getValue(), o1.getValue()))
                                   .map(Entry::getKey)
                                   .collect(Collectors.toList());
        return sorted.indexOf(reader);
    }

    @Override
    @Synchronized
    public Revision getRevision() {
        return revision;
    }
    
    @Override
    public String getScopedStreamName() {
        return scopedSynchronizerStream;
    }
    
    /**
     * Returns the list of segments assigned to the requested reader, or null if this reader does not exist.
     */
    @Synchronized
    Set<Segment> getSegments(String reader) {
        Map<SegmentWithRange, Long> segments = assignedSegments.get(reader);
        if (segments == null) {
            return null;
        }
        return segments.keySet().stream().map(SegmentWithRange::getSegment).collect(Collectors.toSet());
    }
    
    @Synchronized
    @VisibleForTesting
    Map<SegmentWithRange, Long> getAssignedSegments(String reader) {
         return assignedSegments.get(reader);
    }
    
    @Synchronized
    Map<Stream, Map<SegmentWithRange, Long>> getPositions() {
        Map<Stream, Map<SegmentWithRange, Long>> result = new HashMap<>();
        for (Entry<SegmentWithRange, Long> entry : unassignedSegments.entrySet()) {
            result.computeIfAbsent(entry.getKey().getSegment().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }
        for (Map<SegmentWithRange, Long> assigned : assignedSegments.values()) {
            for (Entry<SegmentWithRange, Long> entry : assigned.entrySet()) {
                result.computeIfAbsent(entry.getKey().getSegment().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
    
    @Synchronized
    Map<SegmentWithRange, Long> getLastReadPositions(Stream stream) {
        Map<SegmentWithRange, Long> result = new HashMap<>();
        for (Entry<SegmentWithRange, Long> entry : lastReadPosition.entrySet()) {
            Segment segment = entry.getKey().getSegment();
            if (segment.getScope().equals(stream.getScope()) && segment.getStreamName().equals(stream.getStreamName())) {                
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
    
    @Synchronized
    int getNumberOfUnassignedSegments() {
        return unassignedSegments.size();
    }
    
    @Synchronized
    Map<SegmentWithRange, Long> getUnassignedSegments() {
        return new HashMap<>(unassignedSegments);
    }

    Map<Segment, Long> getEndSegments() {
        return endSegments; //endSegments is an ImmutableMap.
    }

    @Synchronized
    boolean isReaderOnline(String reader) {
        return assignedSegments.get(reader) != null;
    }

    /**
     * Returns the number of segments currently being read from and that are unassigned within the reader group.
     * @return Number of segments.
     */
    @Synchronized
    public int getNumberOfSegments() {
        return assignedSegments.values().stream().mapToInt(Map::size).sum() + unassignedSegments.size();
    }
    
    @Synchronized
    Set<String> getStreamNames() {
        Set<String> result = new HashSet<>();
        for (Map<SegmentWithRange, Long> segments : assignedSegments.values()) {
            for (SegmentWithRange segment : segments.keySet()) {
                result.add(segment.getSegment().getScopedStreamName());
            }
        }
        for (SegmentWithRange segment : unassignedSegments.keySet()) {
            result.add(segment.getSegment().getScopedStreamName());
        }
        return result;
    }
    
    @Synchronized
    String getCheckpointForReader(String readerName) {
        return checkpointState.getCheckpointForReader(readerName);
    }
    
    @Synchronized
    boolean isCheckpointComplete(String checkpointId) {
        return checkpointState.isCheckpointComplete(checkpointId);
    }
    
    @Synchronized
    Map<Segment, Long> getPositionsForCompletedCheckpoint(String checkpointId) {
        return checkpointState.getPositionsForCompletedCheckpoint(checkpointId);
    }

    @Synchronized
    Optional<Map<Stream, StreamCut>> getStreamCutsForCompletedCheckpoint(final String checkpointId) {
        final Optional<Map<Segment, Long>> positionMap = Optional.ofNullable(checkpointState.getPositionsForCompletedCheckpoint(checkpointId));

        return positionMap.map(map -> map.entrySet().stream()
                                         .collect(groupingBy(o -> o.getKey().getStream(),
                                                             collectingAndThen(toMap(Entry::getKey, Entry::getValue),
                                                                               x -> new StreamCutImpl(x.keySet().stream().findAny().get().getStream(), x)))));
    }

    @Synchronized
    Optional<Map<Stream, Map<Segment, Long>>> getPositionsForLastCompletedCheckpoint() {
        Optional<Map<Segment, Long>> positions = checkpointState.getPositionsForLatestCompletedCheckpoint();
        if (positions.isPresent()) {
            Map<Stream, Map<Segment, Long>> result = new HashMap<>();
            for (Entry<Segment, Long> entry : positions.get().entrySet()) {
                result.computeIfAbsent(entry.getKey().getStream(), s -> new HashMap<>()).put(entry.getKey(), entry.getValue());
            }
            return Optional.of(result);
        } else {
            return Optional.empty();
        }
    }
    
    @Synchronized
    boolean hasOngoingCheckpoint() {
        return checkpointState.hasOngoingCheckpoint();
    }

    /**
     * This functions returns true if the readers part of reader group for sealed streams have completely read the data.
     *
     * @return true if end of data. false if there are segments to be read from.
     */
    @Synchronized
    public boolean isEndOfData() {
        return futureSegments.isEmpty() && unassignedSegments.isEmpty() &&
                assignedSegments.values().stream().allMatch(Map::isEmpty);
    }

    @Override
    @Synchronized
    public String toString() {
        StringBuffer sb = new StringBuffer("ReaderGroupState{ ");
        sb.append(checkpointState.toString());
        sb.append(" futureSegments: ");
        sb.append(futureSegments);
        sb.append(" assignedSegments: ");
        sb.append(assignedSegments);
        sb.append(" unassignedSegments: ");
        sb.append(unassignedSegments);
        sb.append(" }");
        return sb.toString();
    }

    @Data
    @Builder
    @RequiredArgsConstructor
    public static class ReaderGroupStateInit implements InitialUpdate<ReaderGroupState> {
        private final ReaderGroupConfig config;
        private final Map<SegmentWithRange, Long> startingSegments;
        private final Map<Segment, Long> endSegments;
        private final boolean updatingConfig;
        
        @Override
        public ReaderGroupState create(String scopedStreamName, Revision revision) {
            return new ReaderGroupState(scopedStreamName, revision, config, startingSegments, endSegments, updatingConfig);
        }
        
        @VisibleForTesting
        static class ReaderGroupStateInitBuilder implements ObjectBuilder<ReaderGroupStateInit> {
        }
        
        static class ReaderGroupStateInitSerializer extends VersionedSerializer.WithBuilder<ReaderGroupStateInit, ReaderGroupStateInitBuilder> {
            @Override
            protected ReaderGroupStateInitBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00)
                          .revision(1, this::write01, this::read01)
                          .revision(2, this::write02, this::read02);
            }

            @VisibleForTesting
            void read00(RevisionDataInput revisionDataInput, ReaderGroupStateInitBuilder builder) throws IOException {
                builder.config(ReaderGroupConfig.fromBytes(ByteBuffer.wrap(revisionDataInput.readArray())));
                ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
                ElementDeserializer<SegmentWithRange> segmentWithRangeDeserializer = in -> new SegmentWithRange(Segment.fromScopedName(in.readUTF()), null);
                builder.startingSegments(revisionDataInput.readMap(segmentWithRangeDeserializer, RevisionDataInput::readLong));
                builder.endSegments(revisionDataInput.readMap(segmentDeserializer, RevisionDataInput::readLong));
            }
            
            private void read01(RevisionDataInput revisionDataInput, ReaderGroupStateInitBuilder builder) throws IOException {
                ElementDeserializer<SegmentWithRange> segmentWithRangeDeserializer = in -> {
                    Segment segment = Segment.fromScopedName(in.readUTF());
                    Range range = readRange(in);
                    return new SegmentWithRange(segment, range);
                };
                builder.startingSegments(revisionDataInput.readMap(segmentWithRangeDeserializer,
                                                                   RevisionDataInput::readLong));
            }

            private void read02(RevisionDataInput revisionDataInput, ReaderGroupStateInitBuilder builder) throws IOException {
                builder.updatingConfig(revisionDataInput.readBoolean());
            }

            @VisibleForTesting
            void write00(ReaderGroupStateInit state, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeBuffer(new ByteArraySegment(state.config.toBytes()));
                ElementSerializer<SegmentWithRange> segmentWithRangeSerializer = (out, s) -> out.writeUTF(s.getSegment().getScopedName());
                ElementSerializer<Segment> segmentSerializer = (out, s) -> out.writeUTF(s.getScopedName());
                revisionDataOutput.writeMap(state.startingSegments, segmentWithRangeSerializer, RevisionDataOutput::writeLong);
                revisionDataOutput.writeMap(state.endSegments, segmentSerializer, RevisionDataOutput::writeLong);
            }
            
            private void write01(ReaderGroupStateInit state, RevisionDataOutput revisionDataOutput) throws IOException {
                ElementSerializer<SegmentWithRange> segmentWithRangeSerializer = (out, s) -> {
                    out.writeUTF(s.getSegment().getScopedName());
                    writeRange(out, s.getRange());
                };
                revisionDataOutput.writeMap(state.startingSegments, segmentWithRangeSerializer, RevisionDataOutput::writeLong);
            }

            private void write02(ReaderGroupStateInit state, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeBoolean(state.updatingConfig);
            }
        }
        
    }
    
    @Data
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class CompactReaderGroupState implements InitialUpdate<ReaderGroupState> {
        @NonNull
        private final ReaderGroupConfig config;
        @NonNull
        private final CheckpointState checkpointState;
        @NonNull
        private final Map<String, Long> distanceToTail;
        @NonNull
        private final Map<SegmentWithRange, Set<Long>> futureSegments;
        @NonNull
        private final Map<String, Map<SegmentWithRange, Long>> assignedSegments;
        private final Map<SegmentWithRange, Long> unassignedSegments;
        @NonNull
        private final Map<SegmentWithRange, Long> lastReadPosition;
        @NonNull
        private final Map<Segment, Long> endSegments;
        private final boolean updatingConfig;
        
        CompactReaderGroupState(ReaderGroupState state) {
            synchronized (state.$lock) {
                config = state.config;
                checkpointState = state.checkpointState.copy();
                distanceToTail = new HashMap<>(state.distanceToTail);
                futureSegments = new HashMap<>();
                for (Entry<SegmentWithRange, Set<Long>> entry : state.futureSegments.entrySet()) {
                    futureSegments.put(entry.getKey(), new HashSet<>(entry.getValue()));
                }
                assignedSegments = new HashMap<>();
                for (Entry<String, Map<SegmentWithRange, Long>> entry : state.assignedSegments.entrySet()) {
                    assignedSegments.put(entry.getKey(), new HashMap<>(entry.getValue()));
                }
                unassignedSegments = new LinkedHashMap<>(state.unassignedSegments);
                lastReadPosition = new HashMap<>(state.lastReadPosition);
                endSegments = state.endSegments;
                updatingConfig = state.updatingConfig;
            }
        }

        @Override
        public ReaderGroupState create(String scopedStreamName, Revision revision) {
            return new ReaderGroupState(scopedStreamName, config, revision, checkpointState, distanceToTail,
                                        futureSegments, assignedSegments, unassignedSegments, lastReadPosition, endSegments, updatingConfig);
        }

        @VisibleForTesting
        static class CompactReaderGroupStateBuilder implements ObjectBuilder<CompactReaderGroupState> {

        }
        
        static class CompactReaderGroupStateSerializer extends VersionedSerializer.WithBuilder<CompactReaderGroupState, CompactReaderGroupStateBuilder> {
            @Override
            protected CompactReaderGroupStateBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00)
                          .revision(1, this::write01, this::read01)
                          .revision(2, this::write02, this::read02);
            }

            private void read00(RevisionDataInput revisionDataInput,
                                CompactReaderGroupStateBuilder builder) throws IOException {
                ElementDeserializer<String> stringDeserializer = RevisionDataInput::readUTF;
                ElementDeserializer<Long> longDeserializer = RevisionDataInput::readLong;
                ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
                ElementDeserializer<SegmentWithRange> segmentWithRangeDeserializer = in -> new SegmentWithRange(Segment.fromScopedName(in.readUTF()),
                                                                                                                null);
                builder.config(ReaderGroupConfig.fromBytes(ByteBuffer.wrap(revisionDataInput.readArray())));
                builder.checkpointState(CheckpointState.fromBytes(ByteBuffer.wrap(revisionDataInput.readArray())));
                builder.distanceToTail(revisionDataInput.readMap(stringDeserializer, longDeserializer));
                builder.futureSegments(revisionDataInput.readMap(segmentWithRangeDeserializer,
                                                                 in -> in.readCollection(RevisionDataInput::readLong,
                                                                                         HashSet::new)));
                builder.assignedSegments(revisionDataInput.readMap(stringDeserializer,
                                                                   in -> in.readMap(segmentWithRangeDeserializer,
                                                                                    longDeserializer)));
                builder.unassignedSegments(revisionDataInput.readMap(segmentWithRangeDeserializer, longDeserializer));
                builder.endSegments(revisionDataInput.readMap(segmentDeserializer, longDeserializer));
            }

            private void read01(RevisionDataInput revisionDataInput,
                                CompactReaderGroupStateBuilder builder) throws IOException {
                
                ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
                Map<Segment, Long> lastReadPos = revisionDataInput.readMap(segmentDeserializer, RevisionDataInput::readLong);              
                Map<Segment, Range> ranges = revisionDataInput.readMap(segmentDeserializer, ReaderGroupState::readRange);

                Function<? super Entry<SegmentWithRange, ?>, SegmentWithRange> keyMapper = e -> new SegmentWithRange(e.getKey().getSegment(),
                                                                                                                     ranges.get(e.getKey().getSegment()));
                Map<SegmentWithRange, Set<Long>> fs = builder.futureSegments.entrySet()
                                                                            .stream()
                                                                            .collect(Collectors.toMap(keyMapper, e -> e.getValue()));
                builder.futureSegments(fs);
                Map<String, Map<SegmentWithRange, Long>> as = new HashMap<>();
                for (Entry<String, Map<SegmentWithRange, Long>> entry : builder.assignedSegments.entrySet()) {
                    as.put(entry.getKey(),
                           entry.getValue()
                                .entrySet()
                                .stream()
                                .collect(Collectors.toMap(keyMapper, e -> e.getValue())));
                }
                builder.assignedSegments(as);
                Map<SegmentWithRange, Long> us = new LinkedHashMap<>();
                for (Entry<SegmentWithRange, Long> e : builder.unassignedSegments.entrySet()) {
                    us.put(new SegmentWithRange(e.getKey().getSegment(), ranges.get(e.getKey().getSegment())),
                           e.getValue());
                }
                builder.unassignedSegments(us);
                builder.lastReadPosition(lastReadPos.entrySet()
                                                    .stream()
                                                    .collect(Collectors.toMap(e -> new SegmentWithRange(e.getKey(),
                                                                                                        ranges.get(e.getKey())),
                                                                              e -> e.getValue())));
            }

            private void read02(RevisionDataInput revisionDataInput,
                                CompactReaderGroupStateBuilder builder) throws IOException {
                builder.updatingConfig(revisionDataInput.readBoolean());
            }

            private void write00(CompactReaderGroupState object, RevisionDataOutput revisionDataOutput) throws IOException {
                ElementSerializer<String> stringSerializer = RevisionDataOutput::writeUTF;
                ElementSerializer<Long> longSerializer = RevisionDataOutput::writeLong;
                ElementSerializer<Segment> segmentSerializer = (out, segment) -> out.writeUTF(segment.getScopedName());
                ElementSerializer<SegmentWithRange> segmentWithRangeSerializer = (out, segment) -> out.writeUTF(segment.getSegment().getScopedName());
                revisionDataOutput.writeBuffer(new ByteArraySegment(object.config.toBytes()));
                revisionDataOutput.writeBuffer(new ByteArraySegment(object.checkpointState.toBytes()));
                revisionDataOutput.writeMap(object.distanceToTail, stringSerializer, longSerializer);
                revisionDataOutput.writeMap(object.futureSegments, segmentWithRangeSerializer,
                                            (out, obj) -> out.writeCollection(obj, RevisionDataOutput::writeLong));
                revisionDataOutput.writeMap(object.assignedSegments, stringSerializer,
                                            (out, obj) -> out.writeMap(obj, segmentWithRangeSerializer, longSerializer));
                revisionDataOutput.writeMap(object.unassignedSegments, segmentWithRangeSerializer, longSerializer);
                revisionDataOutput.writeMap(object.endSegments, segmentSerializer, longSerializer);
            }
            
            private void write01(CompactReaderGroupState object, RevisionDataOutput revisionDataOutput) throws IOException {
                Map<Segment, SegmentWithRange.Range> ranges = new HashMap<>();
                for (Entry<SegmentWithRange, Set<Long>> entry : object.futureSegments.entrySet()) {
                    ranges.put(entry.getKey().getSegment(), entry.getKey().getRange());
                }
                for (Entry<String, Map<SegmentWithRange, Long>> entry : object.assignedSegments.entrySet()) {
                    Set<Entry<SegmentWithRange, Long>> entrySet = entry.getValue().entrySet();
                    for (Entry<SegmentWithRange, Long> e : entrySet) {
                        ranges.put(e.getKey().getSegment(), e.getKey().getRange());
                    }
                }
                for (Entry<SegmentWithRange, Long> e : object.unassignedSegments.entrySet()) {
                    ranges.put(e.getKey().getSegment(), e.getKey().getRange());
                }
                for (Entry<SegmentWithRange, Long> e : object.lastReadPosition.entrySet()) {
                    ranges.put(e.getKey().getSegment(), e.getKey().getRange());
                }
                revisionDataOutput.writeMap(object.lastReadPosition,
                                            (out, segment) -> out.writeUTF(segment.getSegment().getScopedName()),
                                            RevisionDataOutput::writeLong);
                ElementSerializer<Segment> segmentSerializer = (out, segment) -> out.writeUTF(segment.getScopedName());
                revisionDataOutput.writeMap(ranges, segmentSerializer, ReaderGroupState::writeRange);
            }

            private void write02(CompactReaderGroupState object, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeBoolean(object.updatingConfig);
            }
        }
    }
    
    /**
     * Abstract class from which all state updates extend.
     */
    @EqualsAndHashCode
    static abstract class ReaderGroupStateUpdate implements Update<ReaderGroupState> {

        @Override
        public ReaderGroupState applyTo(ReaderGroupState oldState, Revision newRevision) {
            synchronized (oldState.$lock) {
                update(oldState);
                oldState.revision = newRevision;
            }
            return oldState;
        }

        /**
         * Changes the state to reflect the update.
         * Note that a lock while this method is called so only one update will be applied at a time.
         * Implementations of this should not call any methods outside of this class.
         * @param state The state to be updated.
         */
        abstract void update(ReaderGroupState state);
    }
    
    /**
     * Adds a reader to the reader group. (No segments are initially assigned to it)
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class AddReader extends ReaderGroupStateUpdate {
        private final String readerId;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<SegmentWithRange, Long> oldPos = state.assignedSegments.putIfAbsent(readerId, new HashMap<>());
            if (oldPos != null) {
                throw new IllegalStateException("Attempted to add a reader that is already online: " + readerId);
            }
            state.distanceToTail.putIfAbsent(readerId, Long.MAX_VALUE);
        }
        
        private static class AddReaderBuilder implements ObjectBuilder<AddReader> {

        }

        private static class AddReaderSerializer extends VersionedSerializer.WithBuilder<AddReader, AddReaderBuilder> {
            @Override
            protected AddReaderBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, AddReaderBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
            }

            private void write00(AddReader object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
            }
        }
    }
    
    /**
     * Remove a reader from reader group, releasing all segments it owned.
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class RemoveReader extends ReaderGroupStateUpdate {

        private final String readerId;
        private final Map<Segment, Long> ownedSegments;
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<SegmentWithRange, Long> assignedSegments = state.assignedSegments.remove(readerId);
            Map<Segment, Long> finalPositions = new HashMap<>();
            if (assignedSegments != null) {
                val iter = assignedSegments.entrySet().iterator();
                while (iter.hasNext()) {
                    Entry<SegmentWithRange, Long> entry = iter.next();
                    SegmentWithRange segment = entry.getKey();
                    Long offset = ownedSegments.get(segment.getSegment());
                    if (offset == null) {
                        offset = entry.getValue();
                    }
                    finalPositions.put(segment.getSegment(), offset);
                    state.unassignedSegments.put(segment, offset);
                    iter.remove();
                }
            }
            state.distanceToTail.remove(readerId);
            state.checkpointState.removeReader(readerId, finalPositions);
        }
        
        private static class RemoveReaderBuilder implements ObjectBuilder<RemoveReader> {

        }

        private static class RemoveReaderSerializer
                extends VersionedSerializer.WithBuilder<RemoveReader, RemoveReaderBuilder> {
            @Override
            protected RemoveReaderBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, RemoveReaderBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
                builder.ownedSegments(in.readMap(i -> Segment.fromScopedName(i.readUTF()), RevisionDataInput::readLong));
            }

            private void write00(RemoveReader object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
                out.writeMap(object.ownedSegments, (o, segment) -> o.writeUTF(segment.getScopedName()), RevisionDataOutput::writeLong);
            }
        }
    }

    /**
     * Release a currently owned segment.
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class ReleaseSegment extends ReaderGroupStateUpdate {
        
        private final String readerId;
        private final Segment segment;
        private final long offset;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<SegmentWithRange, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            SegmentWithRange segmentWithRange = removeSegmentFromAssigned(assigned);
            state.unassignedSegments.put(segmentWithRange, offset);
            state.lastReadPosition.replace(segmentWithRange, offset);
        }
        
        private SegmentWithRange removeSegmentFromAssigned(Map<SegmentWithRange, Long> assigned) {
            SegmentWithRange result = null; 
            for (Iterator<SegmentWithRange> iterator = assigned.keySet().iterator(); iterator.hasNext();) {
                SegmentWithRange key = iterator.next();
                if (key.getSegment().equals(segment)) {
                    iterator.remove();
                    result = key;
                }
            }
            if (result == null) {
                throw new IllegalStateException(
                        readerId + " asked to release a segment that was not assigned to it " + segment);
            }
            return result;
        }

        private static class ReleaseSegmentBuilder implements ObjectBuilder<ReleaseSegment> {

        }

        private static class ReleaseSegmentSerializer
                extends VersionedSerializer.WithBuilder<ReleaseSegment, ReleaseSegmentBuilder> {
            @Override
            protected ReleaseSegmentBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, ReleaseSegmentBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
                builder.segment(Segment.fromScopedName(in.readUTF()));
                builder.offset(in.readLong());
            }

            private void write00(ReleaseSegment object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
                out.writeUTF(object.segment.getScopedName());
                out.writeLong(object.offset);
            }
        }
    }

    /**
     * Acquire a currently unassigned segment.
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class AcquireSegment extends ReaderGroupStateUpdate {
        private final String readerId;
        private final Segment segment;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<SegmentWithRange, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            SegmentWithRange acquired = null;
            for (SegmentWithRange segmentWithRange : state.unassignedSegments.keySet()) {
                if (segmentWithRange.getSegment().equals(segment)) {
                    acquired = segmentWithRange;
                }
            }
            if (acquired == null) {
                throw new IllegalStateException("Segment: " + segment + " is not unassigned. " + state);
            }
            Long offset = state.unassignedSegments.remove(acquired);
            assigned.put(acquired, offset);
        }

        private static class AcquireSegmentBuilder implements ObjectBuilder<AcquireSegment> {

        }

        private static class AcquireSegmentSerializer
                extends VersionedSerializer.WithBuilder<AcquireSegment, AcquireSegmentBuilder> {
            @Override
            protected AcquireSegmentBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, AcquireSegmentBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
                builder.segment(Segment.fromScopedName(in.readUTF()));
            }

            private void write00(AcquireSegment object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
                out.writeUTF(object.segment.getScopedName());
            }
        }
    }
    
    /**
     * Update the size of this reader's backlog for load balancing purposes. 
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class UpdateDistanceToTail extends ReaderGroupStateUpdate {

        private final String readerId;
        private final long distanceToTail;
        private final Map<SegmentWithRange, Long> lastReadPositions;
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.distanceToTail.put(readerId, Math.max(ASSUMED_LAG_MILLIS, distanceToTail));
            if (lastReadPositions != null) { // if state was serialized via the revision 0 then lastReadPositions will be null.
                for (Entry<SegmentWithRange, Long> entry : lastReadPositions.entrySet()) {
                    state.lastReadPosition.replace(entry.getKey(), entry.getValue());
                }
            }
        }
        
        @VisibleForTesting
        static class UpdateDistanceToTailBuilder implements ObjectBuilder<UpdateDistanceToTail> {

        }

        @VisibleForTesting
        static class UpdateDistanceToTailSerializer
                extends VersionedSerializer.WithBuilder<UpdateDistanceToTail, UpdateDistanceToTailBuilder> {
            @Override
            protected UpdateDistanceToTailBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00)
                          .revision(1, this::write01, this::read01);
            }

            @VisibleForTesting
            void read00(RevisionDataInput in, UpdateDistanceToTailBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
                builder.distanceToTail(in.readLong());
            }
            
            private void read01(RevisionDataInput revisionDataInput, UpdateDistanceToTailBuilder builder) throws IOException {
                Map<SegmentWithRange, Long> positions = revisionDataInput.readMap(in -> new SegmentWithRange(Segment.fromScopedName(in.readUTF()),
                                                                                                             readRange(in)),
                                                                                  RevisionDataInput::readLong);
                builder.lastReadPositions(positions);
            }

            @VisibleForTesting
            void write00(UpdateDistanceToTail object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
                out.writeLong(object.distanceToTail);
            }
            
            private void write01(UpdateDistanceToTail object, RevisionDataOutput revisionDataOutput) throws IOException {
                ElementSerializer<SegmentWithRange> segmentWithRangeSerializer = (out, s) -> {
                    out.writeUTF(s.getSegment().getScopedName());
                    writeRange(out, s.getRange());
                };
                revisionDataOutput.writeMap(object.lastReadPositions, segmentWithRangeSerializer, RevisionDataOutput::writeLong);
            }
        }
    }
    
    /**
     * Updates a position object when the reader has completed a segment.
     */
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class SegmentCompleted extends ReaderGroupStateUpdate {

        private final String readerId;
        private final SegmentWithRange segmentCompleted;
        private final Map<SegmentWithRange, List<Long>> successorsMappedToTheirPredecessors; //Immutable
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<SegmentWithRange, Long> assigned = state.assignedSegments.get(readerId);
            Preconditions.checkState(assigned != null, "%s is not part of the readerGroup", readerId);
            if (assigned.remove(segmentCompleted) == null) {
                throw new IllegalStateException(
                        readerId + " asked to complete a segment that was not assigned to it " + segmentCompleted);
            }
            state.lastReadPosition.remove(segmentCompleted);
            for (Entry<SegmentWithRange, List<Long>> entry : successorsMappedToTheirPredecessors.entrySet()) {
                if (!state.futureSegments.containsKey(entry.getKey())) {
                    Set<Long> requiredToComplete = new HashSet<>(entry.getValue());
                    state.futureSegments.put(entry.getKey(), requiredToComplete);
                }
            }
            for (Set<Long> requiredToComplete : state.futureSegments.values()) {
                requiredToComplete.remove(segmentCompleted.getSegment().getSegmentId());
            }
            val iter = state.futureSegments.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<SegmentWithRange, Set<Long>> entry = iter.next();
                if (entry.getValue().isEmpty()) {
                    state.unassignedSegments.put(entry.getKey(), 0L);
                    state.lastReadPosition.putIfAbsent(entry.getKey(), 0L);
                    iter.remove();
                }
            }
        }

        private static class SegmentCompletedBuilder implements ObjectBuilder<SegmentCompleted> {

        }

        private static class SegmentCompletedSerializer
                extends VersionedSerializer.WithBuilder<SegmentCompleted, SegmentCompletedBuilder> {
            @Override
            protected SegmentCompletedBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00)
                          .revision(1, this::write01, this::read01);
            }

            private void read00(RevisionDataInput in, SegmentCompletedBuilder builder) throws IOException {
                builder.readerId(in.readUTF());
                builder.segmentCompleted(new SegmentWithRange(Segment.fromScopedName(in.readUTF()), null));
                builder.successorsMappedToTheirPredecessors(in.readMap(i -> new SegmentWithRange(Segment.fromScopedName(i.readUTF()), null),
                                                                       i -> i.readCollection(RevisionDataInput::readLong, ArrayList::new)));
            }
            
            private void read01(RevisionDataInput revisionDataInput, SegmentCompletedBuilder builder) throws IOException {
                ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
                Map<Segment, Range> ranges = revisionDataInput.readMap(segmentDeserializer, ReaderGroupState::readRange);
                builder.segmentCompleted(new SegmentWithRange(builder.segmentCompleted.getSegment(), ranges.get(builder.segmentCompleted.getSegment())));
                Map<SegmentWithRange, List<Long>> successorsMappedToTheirPredecessors = new HashMap<>();
                for (Entry<SegmentWithRange, List<Long>> entry : builder.successorsMappedToTheirPredecessors.entrySet()) {
                    Segment segment = entry.getKey().getSegment();
                    successorsMappedToTheirPredecessors.put(new SegmentWithRange(segment, ranges.get(segment)), entry.getValue());
                }
                builder.successorsMappedToTheirPredecessors(successorsMappedToTheirPredecessors);
            }

            private void write00(SegmentCompleted object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.readerId);
                out.writeUTF(object.segmentCompleted.getSegment().getScopedName());
                out.writeMap(object.successorsMappedToTheirPredecessors,
                             (o, segment) -> o.writeUTF(segment.getSegment().getScopedName()),
                             (o, predecessors) -> o.writeCollection(predecessors, RevisionDataOutput::writeLong));
            }
            
            private void write01(SegmentCompleted object, RevisionDataOutput out) throws IOException {
                Map<Segment, SegmentWithRange.Range> rangeMap = new HashMap<>();
                rangeMap.put(object.segmentCompleted.getSegment(), object.segmentCompleted.getRange());
                for (SegmentWithRange segment : object.successorsMappedToTheirPredecessors.keySet()) {
                    rangeMap.put(segment.getSegment(), segment.getRange());
                }
                out.writeMap(rangeMap, (o, segment) -> o.writeUTF(segment.getScopedName()), ReaderGroupState::writeRange);
            }
        }
    }
    
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class CheckpointReader extends ReaderGroupStateUpdate {
        private final String checkpointId;
        private final String readerId;
        private final Map<Segment, Long> positions; //Immutable
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.readerCheckpointed(checkpointId, readerId, positions);
            /*
             * Update the assignedSegments only for normal Checkpoints. This should not be updated
             * for silent Checkpoints as it would cause a different offset to be used when the reader goes
             * offline. The current contract is that last checkpointed offset or the provided position will
             * be used when {@link io.pravega.client.stream.ReaderGroup#readerOffline(String, Position)} is invoked.
             */
            if (!state.checkpointState.isCheckpointSilent(checkpointId)) {
                // Each reader updates the offsets of its assigned segments with the current positions for this checkpoint.
                final Map<SegmentWithRange, Long> readerPositions = state.assignedSegments.get(readerId);
                for (Entry<SegmentWithRange, Long> position : readerPositions.entrySet()) {
                    Long offset = positions.get(position.getKey().getSegment());
                    if (offset != null) {
                        position.setValue(offset);
                    }
                }
            }
        }
        
        private static class CheckpointReaderBuilder implements ObjectBuilder<CheckpointReader> {

        }

        private static class CheckpointReaderSerializer
                extends VersionedSerializer.WithBuilder<CheckpointReader, CheckpointReaderBuilder> {
            @Override
            protected CheckpointReaderBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, CheckpointReaderBuilder builder) throws IOException {
                builder.checkpointId(in.readUTF());
                builder.readerId(in.readUTF());
                builder.positions(in.readMap(i -> Segment.fromScopedName(i.readUTF()), RevisionDataInput::readLong));
            }

            private void write00(CheckpointReader object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.checkpointId);
                out.writeUTF(object.readerId);
                out.writeMap(object.positions, (o, segment) -> o.writeUTF(segment.getScopedName()), RevisionDataOutput::writeLong);
            }
        }
    }
    
    @Builder
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    static class CreateCheckpoint extends ReaderGroupStateUpdate {
        @Getter
        private final String checkpointId;

        CreateCheckpoint() {
            this(UUID.randomUUID().toString());
        }
        
        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            Map<Segment, Long> unassignedSegments = state.getUnassignedSegments()
                                                         .entrySet()
                                                         .stream()
                                                         .collect(Collectors.toMap(e -> e.getKey().getSegment(),
                                                                                   e -> e.getValue()));
            state.checkpointState.beginNewCheckpoint(checkpointId, state.getOnlineReaders(), unassignedSegments);
        }

        private static class CreateCheckpointBuilder implements ObjectBuilder<CreateCheckpoint> {

        }

        private static class CreateCheckpointSerializer
                extends VersionedSerializer.WithBuilder<CreateCheckpoint, CreateCheckpointBuilder> {
            @Override
            protected CreateCheckpointBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, CreateCheckpointBuilder builder) throws IOException {
                builder.checkpointId(in.readUTF());
            }

            private void write00(CreateCheckpoint object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.checkpointId);
            }
        }
    }
    
    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class ClearCheckpointsBefore extends ReaderGroupStateUpdate {
        private final String clearUpToCheckpoint;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.clearCheckpointsBefore(clearUpToCheckpoint);
        }

        private static class ClearCheckpointsBeforeBuilder implements ObjectBuilder<ClearCheckpointsBefore> {

        }


        private static class ClearCheckpointsBeforeSerializer
                extends VersionedSerializer.WithBuilder<ClearCheckpointsBefore, ClearCheckpointsBeforeBuilder> {
            @Override
            protected ClearCheckpointsBeforeBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, ClearCheckpointsBeforeBuilder builder) throws IOException {
                builder.clearUpToCheckpoint(in.readUTF());
            }

            private void write00(ClearCheckpointsBefore object, RevisionDataOutput out) throws IOException {
                out.writeUTF(object.clearUpToCheckpoint);
            }
        }
    }

    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class RemoveOutstandingCheckpoints extends ReaderGroupStateUpdate {

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.removeOutstandingCheckpoints();
        }

        private static class RemoveOutstandingCheckpointsBuilder implements ObjectBuilder<RemoveOutstandingCheckpoints> {

        }


        private static class RemoveOutstandingCheckpointsSerializer
                extends VersionedSerializer.WithBuilder<RemoveOutstandingCheckpoints, RemoveOutstandingCheckpointsBuilder> {
            @Override
            protected RemoveOutstandingCheckpointsBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

           private void read00(RevisionDataInput in, RemoveOutstandingCheckpointsBuilder builder) throws IOException {
               builder.build();
            }

            private void write00(RemoveOutstandingCheckpoints object, RevisionDataOutput out) throws IOException {
            }
        }
    }

    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class UpdateCheckpointPublished extends ReaderGroupStateUpdate {
        private final boolean isCheckpointPublishedToController;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.checkpointState.setLastCheckpointPublished(isCheckpointPublishedToController);
        }

        private static class UpdateCheckpointPublishedBuilder implements ObjectBuilder<UpdateCheckpointPublished> {

        }

        private static class UpdateCheckpointPublishedSerializer
                extends VersionedSerializer.WithBuilder<UpdateCheckpointPublished, UpdateCheckpointPublishedBuilder> {
            @Override
            protected UpdateCheckpointPublishedBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, UpdateCheckpointPublishedBuilder builder) throws IOException {
                builder.isCheckpointPublishedToController(in.readBoolean());
            }

            private void write00(UpdateCheckpointPublished object, RevisionDataOutput out) throws IOException {
                out.writeBoolean(object.isCheckpointPublishedToController);
            }
        }
    }

    @Builder
    @Data
    @EqualsAndHashCode(callSuper = false)
    static class UpdatingConfig extends ReaderGroupStateUpdate {
        private final boolean updatingConfig;

        /**
         * @see ReaderGroupState.ReaderGroupStateUpdate#update(ReaderGroupState)
         */
        @Override
        void update(ReaderGroupState state) {
            state.setUpdatingConfig(updatingConfig);
        }

        private static class UpdatingConfigBuilder implements ObjectBuilder<UpdatingConfig> {

        }

        private static class UpdatingConfigSerializer extends
                VersionedSerializer.WithBuilder<UpdatingConfig, UpdatingConfigBuilder> {
            @Override
            protected UpdatingConfigBuilder newBuilder() {
                return builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput in, UpdatingConfigBuilder builder) throws IOException {
                builder.updatingConfig(in.readBoolean());
            }

            private void write00(UpdatingConfig object, RevisionDataOutput out) throws IOException {
                out.writeBoolean(object.updatingConfig);
            }
        }
    }

    public static class ReaderGroupInitSerializer
            extends VersionedSerializer.MultiType<InitialUpdate<ReaderGroupState>> {
        @Override
        protected void declareSerializers(Builder b) {
            b.serializer(ReaderGroupStateInit.class, 0, new ReaderGroupStateInit.ReaderGroupStateInitSerializer())
             .serializer(CompactReaderGroupState.class, 1, new CompactReaderGroupState.CompactReaderGroupStateSerializer());
        }
    }

    public static class ReaderGroupUpdateSerializer extends VersionedSerializer.MultiType<Update<ReaderGroupState>> {
        @Override
        protected void declareSerializers(Builder b) {
            // Declare sub-serializers here. IDs must be unique, non-changeable (during refactoring)
            // and not necessarily sequential or contiguous.
            b.serializer(ReaderGroupStateInit.class, 0, new ReaderGroupStateInit.ReaderGroupStateInitSerializer())
             .serializer(CompactReaderGroupState.class, 1, new CompactReaderGroupState.CompactReaderGroupStateSerializer())
             .serializer(AddReader.class, 2, new AddReader.AddReaderSerializer())
             .serializer(RemoveReader.class, 3, new RemoveReader.RemoveReaderSerializer())
             .serializer(ReleaseSegment.class, 4, new ReleaseSegment.ReleaseSegmentSerializer())
             .serializer(AcquireSegment.class, 5, new AcquireSegment.AcquireSegmentSerializer())
             .serializer(UpdateDistanceToTail.class, 6, new UpdateDistanceToTail.UpdateDistanceToTailSerializer())
             .serializer(SegmentCompleted.class, 7, new SegmentCompleted.SegmentCompletedSerializer())
             .serializer(CheckpointReader.class, 8, new CheckpointReader.CheckpointReaderSerializer())
             .serializer(CreateCheckpoint.class, 9, new CreateCheckpoint.CreateCheckpointSerializer())
             .serializer(ClearCheckpointsBefore.class, 10, new ClearCheckpointsBefore.ClearCheckpointsBeforeSerializer())
             .serializer(UpdateCheckpointPublished.class, 11, new UpdateCheckpointPublished.UpdateCheckpointPublishedSerializer())
             .serializer(UpdatingConfig.class, 12, new UpdatingConfig.UpdatingConfigSerializer())
             .serializer(RemoveOutstandingCheckpoints.class, 13, new RemoveOutstandingCheckpoints.RemoveOutstandingCheckpointsSerializer());
        }
    }
    
    private static void writeRange(RevisionDataOutput out, Range range) throws IOException {
        double low, high;
        if (range == null) {
            low = -1;
            high = -1;
        } else {
            low = range.getLow();
            high = range.getHigh();
        }
        out.writeDouble(low);
        out.writeDouble(high);
    }
    
    private static Range readRange(RevisionDataInput in) throws IOException {
        double low = in.readDouble();
        double high = in.readDouble();
        return (low < 0 || high < 0) ? null : new Range(low, high);
    }
    
}
