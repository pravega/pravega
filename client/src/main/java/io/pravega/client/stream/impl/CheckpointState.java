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
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.io.serialization.RevisionDataInput.ElementDeserializer;
import io.pravega.common.io.serialization.RevisionDataOutput.ElementSerializer;
import io.pravega.common.util.ByteArraySegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.stream.impl.ReaderGroupImpl.SILENT;

@NotThreadSafe
@EqualsAndHashCode
@Slf4j
public class CheckpointState {

    private static final CheckpointStateSerializer SERIALIZER = new CheckpointStateSerializer();

    private final List<String> checkpoints;
    /**
     * Maps CheckpointId to remaining hosts.
     */
    private final Map<String, List<String>> uncheckpointedHosts;

    /**
     * Reverse of {@link #uncheckpointedHosts}. (derived).
     * Maps hosts to their outstanding checkpoints.
     */
    private Map<String, List<String>> checkpointIndex;

    /**
     *  Maps CheckpointId to positions in segments.
     */
    private final Map<String, Map<Segment, Long>> checkpointPositions;

    private Map<Segment, Long> lastCheckpointPosition;

    @Getter
    @Setter
    private boolean isLastCheckpointPublished = true;

    public CheckpointState() {
        this(new ArrayList<>(), new HashMap<>(), new HashMap<>(), null, true);
    }

    @Builder
    private CheckpointState(List<String> checkpoints, Map<String, List<String>> uncheckpointedHosts,
            Map<String, Map<Segment, Long>> checkpointPositions, Map<Segment, Long> lastCheckpointPosition,
                            boolean isLastCheckpointPublished) {
        Preconditions.checkNotNull(checkpoints);
        Preconditions.checkNotNull(uncheckpointedHosts);
        Preconditions.checkNotNull(checkpointPositions);
        this.checkpoints = checkpoints;
        this.uncheckpointedHosts = uncheckpointedHosts;
        this.checkpointPositions = checkpointPositions;
        this.lastCheckpointPosition = lastCheckpointPosition;
        this.isLastCheckpointPublished = isLastCheckpointPublished;
        recomputeCheckpointIndex();
    }

    void beginNewCheckpoint(String checkpointId, Set<String> currentReaders, Map<Segment, Long> knownPositions) {
        if (!checkpointPositions.containsKey(checkpointId)) {
            if (!currentReaders.isEmpty()) {
                uncheckpointedHosts.put(checkpointId, new ArrayList<>(currentReaders));
            }
            checkpointPositions.put(checkpointId, new HashMap<>(knownPositions));
            checkpoints.add(checkpointId);
        }
        recomputeCheckpointIndex();
    }

    String getCheckpointForReader(String readerName) {
        List<String> checkpointsForReader = getCheckpointsForReader(readerName);
        return (checkpointsForReader.isEmpty()) ? null : checkpointsForReader.get(0);
    }

    private List<String> getCheckpointsForReader(String readerName) {
        return checkpointIndex.getOrDefault(readerName, Collections.emptyList());
    }

    private void recomputeCheckpointIndex() {
        checkpointIndex = new HashMap<>();
        for (Entry<String, List<String>> entry : uncheckpointedHosts.entrySet()) {
            String checkpointId = entry.getKey();
            for (String host : entry.getValue()) {
                List<String> checkpointsForHost = checkpointIndex.computeIfAbsent(host, k -> new ArrayList<>());
                checkpointsForHost.add(checkpointId);
            }
        }
    }

    void removeReader(String readerName, Map<Segment, Long> position) {
        List<String> toCheckpoint = checkpointIndex.remove(readerName);
        if (toCheckpoint != null) {
            for (String checkpointId : toCheckpoint) {
                readerCheckpointed(checkpointId, readerName, position);
            }
        }
    }

    void readerCheckpointed(String checkpointId, String readerName, Map<Segment, Long> position) {
        log.debug("Reader : {} completed checkpointing for Checkpoint : {}", readerName, checkpointId);
        List<String> readers = uncheckpointedHosts.get(checkpointId);
        if (readers != null) {
            boolean removed = readers.remove(readerName);
            Preconditions.checkState(removed, "Reader already checkpointed.");
            List<String> cps = checkpointIndex.get(readerName);
            if (cps != null) {
                cps.remove(checkpointId);
            }
            Map<Segment, Long> positions = checkpointPositions.get(checkpointId);
            positions.putAll(position);
            if (readers.isEmpty()) {
                uncheckpointedHosts.remove(checkpointId);
                log.info("Completed checkpointing for Checkpoint : {} by all readers", checkpointId);
                //store last checkpoint position if it is not a stream-cut (silent checkpoint)
                if (!isCheckpointSilent(checkpointId)) {
                    //checkpoint operation completed for all readers, update the last checkpoint position.
                    lastCheckpointPosition = checkpointPositions.get(checkpointId);
                    isLastCheckpointPublished = false;
                }
            }
        }
    }

    boolean isCheckpointComplete(String checkpointId) {
        return !uncheckpointedHosts.containsKey(checkpointId);
    }

    boolean isCheckpointSilent(String checkpointId) {
        return checkpointId.contains(SILENT);
    }

    Map<Segment, Long> getPositionsForCompletedCheckpoint(String checkpointId) {
        if (uncheckpointedHosts.containsKey(checkpointId)) {
            return null;
        }
        return checkpointPositions.get(checkpointId);
    }

    Optional<Map<Segment, Long>> getPositionsForLatestCompletedCheckpoint() {
        return Optional.ofNullable(lastCheckpointPosition);
    }

    boolean hasOngoingCheckpoint() {
        return !uncheckpointedHosts.isEmpty();
    }

    /**
     * Get the outstanding Checkpoints. It should not take silent Checkpoints into account.
     * @return the List of outstanding Checkpoints.
     */
    List<String> getOutstandingCheckpoints() {
        return checkpoints.stream()
                          .filter(checkpoint -> !(isCheckpointSilent(checkpoint) || isCheckpointComplete(checkpoint)))
                          .collect(Collectors.toList());
    }

    /**
     * Removes the outstanding checkpoints.
     */
    void removeOutstandingCheckpoints() {
       List<String> checkpoint = getOutstandingCheckpoints();
       for (String cp:checkpoint) {
           uncheckpointedHosts.remove(cp);
           checkpointPositions.remove(cp);
       }
       recomputeCheckpointIndex();
       log.info("Outstanding checkpoints are cleared successfully");
    }

    /**
     * Get the map of CheckpointId to list of readers blocking that checkpoint.
     * @return the map.
     */
    Map<String, List<String>> getReaderBlockingCheckpointsMap() {
        return  uncheckpointedHosts.entrySet().stream()
                                   .filter(checkpoint -> !(isCheckpointSilent(checkpoint.getKey()) || isCheckpointComplete(checkpoint.getKey())))
                                   .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    void clearCheckpointsBefore(String checkpointId) {
        if (checkpointPositions.containsKey(checkpointId)) {
            for (Iterator<String> iterator = checkpoints.iterator(); iterator.hasNext();) {
                String cp = iterator.next();
                if (cp.equals(checkpointId)) {
                    break;
                }
                uncheckpointedHosts.remove(cp);
                checkpointPositions.remove(cp);
                iterator.remove();
            }
            recomputeCheckpointIndex();
        }
    }

    /**
     * @return A copy of this object
     */
    CheckpointState copy() {
        List<String> cps = new ArrayList<>(checkpoints);
        Map<String, List<String>> ucph = new HashMap<>(uncheckpointedHosts.size());
        uncheckpointedHosts.forEach((cp, hosts) -> ucph.put(cp, new ArrayList<>(hosts)));
        Map<String, Map<Segment, Long>> cpps = new HashMap<>();
        checkpointPositions.forEach((cp, pos) -> cpps.put(cp, new HashMap<>(pos)));
        Map<Segment, Long> lcp = lastCheckpointPosition == null ? null : new HashMap<>(lastCheckpointPosition);
        return new CheckpointState(cps, ucph, cpps, lcp, isLastCheckpointPublished);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CheckpointState { ongoingCheckpoints: ");
        sb.append(checkpoints.toString());
        sb.append(",  readersBlockingEachCheckpoint: ");
        sb.append(uncheckpointedHosts.toString());
        sb.append(" }");
        return sb.toString();
    }

    @VisibleForTesting
    static class CheckpointStateBuilder implements ObjectBuilder<CheckpointState> {

    }

    private static class CheckpointStateSerializer
            extends VersionedSerializer.WithBuilder<CheckpointState, CheckpointStateBuilder> {
        @Override
        protected CheckpointStateBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
            version(0).revision(1, this::write01, this::read01);
        }

        private void read00(RevisionDataInput input, CheckpointStateBuilder builder) throws IOException {
            ElementDeserializer<String> stringDeserializer = RevisionDataInput::readUTF;
            ElementDeserializer<Long> longDeserializer = RevisionDataInput::readLong;
            ElementDeserializer<Segment> segmentDeserializer = in -> Segment.fromScopedName(in.readUTF());
            builder.checkpoints(input.readCollection(stringDeserializer, ArrayList::new));
            builder.uncheckpointedHosts(input.readMap(stringDeserializer, in -> in.readCollection(stringDeserializer, ArrayList::new)));
            builder.checkpointPositions(input.readMap(stringDeserializer, in -> in.readMap(segmentDeserializer, longDeserializer)));
            builder.lastCheckpointPosition(input.readMap(segmentDeserializer, longDeserializer));
        }

        private void write00(CheckpointState object, RevisionDataOutput output) throws IOException {
            ElementSerializer<String> stringSerializer = RevisionDataOutput::writeUTF;
            ElementSerializer<Long> longSerializer = RevisionDataOutput::writeLong;
            ElementSerializer<Segment> segmentSerializer = (out, segment) -> out.writeUTF(segment.getScopedName());
            output.writeCollection(object.checkpoints, stringSerializer);
            output.writeMap(object.uncheckpointedHosts, stringSerializer, (out, hosts) -> out.writeCollection(hosts, stringSerializer));
            output.writeMap(object.checkpointPositions, stringSerializer, (out, map) -> out.writeMap(map, segmentSerializer, longSerializer));
            output.writeMap(object.lastCheckpointPosition, segmentSerializer, longSerializer);
        }

        private void read01(RevisionDataInput input, CheckpointStateBuilder builder) throws IOException {
            builder.isLastCheckpointPublished(input.readBoolean());
        }

        private void write01(CheckpointState object, RevisionDataOutput output) throws IOException {
            output.writeBoolean(object.isLastCheckpointPublished);
        }
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static CheckpointState fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }
}
