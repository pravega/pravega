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
package io.pravega.client.stream;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataInput.ElementDeserializer;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.RevisionDataOutput.ElementSerializer;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.summarizingLong;

@Data
@Builder(toBuilder = true)
@EqualsAndHashCode
public class ReaderGroupConfig implements Serializable {

    public static final UUID DEFAULT_UUID = new UUID(0L, 0L);
    public static final long DEFAULT_GENERATION = -1;
    private static final long serialVersionUID = 1L;
    private static final ReaderGroupConfigSerializer SERIALIZER = new ReaderGroupConfigSerializer();
    private final long groupRefreshTimeMillis;
    @Getter
    private final long automaticCheckpointIntervalMillis;

    private final Map<Stream, StreamCut> startingStreamCuts;
    private final Map<Stream, StreamCut> endingStreamCuts;

    private final int maxOutstandingCheckpointRequest;

    private final StreamDataRetention retentionType;

    @EqualsAndHashCode.Exclude
    private final long generation;
    @EqualsAndHashCode.Exclude
    private final UUID readerGroupId;
    /**
     * If a Reader Group wants unconsumed data to be retained in a Stream,
     * the retentionType in {@link ReaderGroupConfig} should be set to
     * to 'MANUAL_RELEASE_AT_USER_STREAMCUT' or 'AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT'.
     * Setting these options implies the Reader Group will supply its consumption {@link StreamCut},
     * so only un-consumed data can be retained.
     * This notification can be manual ('MANUAL_RELEASE_AT_USER_STREAMCUT') or automatic ('AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT')
     * To ensure Reader Groups' read positions do not impact data retention in the Stream, set retentionType='NONE',
     * so consumption position notifications are not sent to Controller.
     *
     * NONE - Set when the reader's positions of this Reader Group should not impact Stream retention.
     * MANUAL_RELEASE_AT_USER_STREAMCUT - User provides StreamCut to mark position in the Stream till which data is consumed using {@link ReaderGroup#updateRetentionStreamCut(java.util.Map) } API.
     * AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT - StreamCut of the last completed checkpoint is used as the retention Stream-Cut.
     * */
    public enum StreamDataRetention {
        NONE,
        MANUAL_RELEASE_AT_USER_STREAMCUT,
        AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT;
    }

    public static ReaderGroupConfig cloneConfig(final ReaderGroupConfig configToClone, final UUID readerGroupId, final long generation) {
        return ReaderGroupConfig.builder()
                .readerGroupId(readerGroupId)
                .generation(generation)
                .retentionType(configToClone.getRetentionType())
                .automaticCheckpointIntervalMillis(configToClone.getAutomaticCheckpointIntervalMillis())
                .groupRefreshTimeMillis(configToClone.getGroupRefreshTimeMillis())
                .maxOutstandingCheckpointRequest(configToClone.getMaxOutstandingCheckpointRequest())
                .startingStreamCuts(configToClone.getStartingStreamCuts())
                .endingStreamCuts(configToClone.getEndingStreamCuts())
                .build();
    }

    public static class ReaderGroupConfigBuilder implements ObjectBuilder<ReaderGroupConfig> {
       private long groupRefreshTimeMillis = 3000; //default value
       private long automaticCheckpointIntervalMillis = 30000; //default value
       // maximum outstanding checkpoint request that is allowed at any given time.
       private int maxOutstandingCheckpointRequest = 3; //default value
       private StreamDataRetention retentionType = StreamDataRetention.NONE; //default value
       private long generation = DEFAULT_GENERATION;
       private UUID readerGroupId = DEFAULT_UUID;

       private ReaderGroupConfigBuilder readerGroupId(UUID initReaderGroupId) {
           this.readerGroupId = initReaderGroupId;
           return this;
       }

       private ReaderGroupConfigBuilder generation(final long gen) {
           this.generation = gen;
           return this;
       }
       
       /**
        * Disables automatic checkpointing. Checkpoints need to be
        * generated manually, see this method:
        *
        * {@link ReaderGroup#initiateCheckpoint(String, ScheduledExecutorService)}.
        *
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder disableAutomaticCheckpoints() {
           this.automaticCheckpointIntervalMillis = -1;
           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} and end {@link StreamCut} to be read by the
        * readers of a ReaderGroup.
        *
        * @param scopedStreamName Scoped Name of the Stream.
        * @param startStreamCut Start {@link StreamCut}
        * @param endStreamCut End {@link StreamCut}
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scopedStreamName, final StreamCut startStreamCut, final StreamCut endStreamCut) {
           final Stream stream = Stream.of(scopedStreamName);

           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(stream, startStreamCut);

           if (endingStreamCuts == null) {
               endingStreamCuts = new HashMap<>();
           }
           this.endingStreamCuts.put(stream, endStreamCut);

           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        * @param scopedStreamName Scoped Name of the Stream.
        * @param startStreamCut Start {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scopedStreamName, final StreamCut startStreamCut) {
           return stream(scopedStreamName, startStreamCut, StreamCut.UNBOUNDED);
       }


       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        * @param scopedStreamName Stream name.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scopedStreamName) {
           return stream(scopedStreamName, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream and its associated start {@link StreamCut} and end {@link StreamCut} to be read by
        * the readers of a ReaderGroup.
        *
        * @param stream Stream.
        * @param startStreamCut Start {@link StreamCut}.
        * @param endStreamCut End {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream, final StreamCut startStreamCut, final StreamCut endStreamCut) {
           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(stream, startStreamCut);

           if (endingStreamCuts == null) {
               endingStreamCuts = new HashMap<>();
           }
           this.endingStreamCuts.put(stream, endStreamCut);

           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        * @param stream Stream.
        * @param startStreamCut Start {@link StreamCut}
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream, final StreamCut startStreamCut) {
            return stream(stream, startStreamCut, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        * @param stream Stream.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream) {
           return stream(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
       }

       /**
        * Ensure the readers of the ReaderGroup start from this provided streamCuts.
        * @param streamCuts Map of {@link Stream} and its corresponding {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromStreamCuts(final Map<Stream, StreamCut> streamCuts) {
           this.startingStreamCuts(streamCuts);
           return this;
       }

       /**
        * Ensure the readers of the ReaderGroup start from the provided {@link Checkpoint}.
        * @param checkpoint {@link Checkpoint}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromCheckpoint(final Checkpoint checkpoint) {
           this.startingStreamCuts(checkpoint.asImpl().getPositions());
           return this;
       }

       /**
        * Set the retention config for the {@link ReaderGroup}.
        * For Consumption based retention of data in the Stream, this field should be set to
        * MANUAL_RELEASE_AT_USER_STREAMCUT or AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT.
        * See: {@link ReaderGroupConfig.StreamDataRetention}
        * Default value: NONE.
        *
        * @param type A type parameter that indicates if the reads of this ReaderGroup would impact retention of data in the Stream. {@link ReaderGroupConfig.StreamDataRetention}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder retentionType(StreamDataRetention type) {
           this.retentionType = type;
           return this;
       }

       @Override
       public ReaderGroupConfig build() {
           checkArgument(startingStreamCuts != null && startingStreamCuts.size() > 0,
                   "Stream names that the reader group can read from cannot be empty");

           //endStreamCuts is an optional configuration. Initialize if it is null.
           if (endingStreamCuts == null) {
               endingStreamCuts = Collections.emptyMap();
           }

           //validate start and end StreamCuts
           validateStreamCut(startingStreamCuts);
           validateStreamCut(endingStreamCuts);

           //basic check to verify if endStreamCut > startStreamCut.
           validateStartAndEndStreamCuts(startingStreamCuts, endingStreamCuts);

           //basic check to verify if maxOutstandingCheckpointRequest value > 0
           Preconditions.checkArgument(maxOutstandingCheckpointRequest > 0,
                   "Outstanding checkpoint request should be greater than zero");

           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis,
                   startingStreamCuts, endingStreamCuts, maxOutstandingCheckpointRequest, retentionType,
                   generation, readerGroupId);
       }

       private void validateStartAndEndStreamCuts(Map<Stream, StreamCut> startStreamCuts,
                                                  Map<Stream, StreamCut> endStreamCuts) {
           endStreamCuts.entrySet().stream().filter(e -> !e.getValue().equals(StreamCut.UNBOUNDED))
                                .forEach(e -> {
                               if (startStreamCuts.get(e.getKey()) != StreamCut.UNBOUNDED) {
                                   verifyStartAndEndStreamCuts(startStreamCuts.get(e.getKey()), e.getValue());
                               }
                           });
       }

       /**
        * Verify if startStreamCut comes before endStreamCut.
        */
       private void verifyStartAndEndStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut) {
           final Map<Segment, Long> startPositions = startStreamCut.asImpl().getPositions();
           final Map<Segment, Long> endPositions = endStreamCut.asImpl().getPositions();
           //check offsets for overlapping segments.
           startPositions.keySet().stream().filter(endPositions::containsKey)
                        .forEach(s -> {
                            if (startPositions.get(s) == -1) {
                                Preconditions.checkArgument(endPositions.get(s) == -1,
                                                            "Segment offset in startStreamCut should be <= segment offset in endStreamCut");
                            } else if (endPositions.get(s) != -1) {
                                Preconditions.checkArgument(startPositions.get(s) <= endPositions.get(s),
                                                            "Segment offset in startStreamCut should be <= segment offset in endStreamCut.");
                            }
                        });

           val fromSCSummary = startPositions.keySet()
                                             .stream().collect(summarizingLong(Segment::getSegmentId));
           val toSCSummary = endPositions.keySet()
                                         .stream().collect(summarizingLong(Segment::getSegmentId));
           Preconditions.checkArgument(fromSCSummary.getMin() <= toSCSummary.getMin(),
                   "Start stream cut must precede end stream cut.");
           Preconditions.checkArgument(fromSCSummary.getMax() <= toSCSummary.getMax(),
                   "Start stream cut must precede end stream cut.");
       }

       private void validateStreamCut(Map<Stream, StreamCut> streamCuts) {
           streamCuts.forEach((s, streamCut) -> {
               if (!streamCut.equals(StreamCut.UNBOUNDED)) {
                   checkArgument(s.equals(streamCut.asImpl().getStream()));
               }
           });
       }
    }

    private static class ReaderGroupConfigSerializer
            extends VersionedSerializer.WithBuilder<ReaderGroupConfig, ReaderGroupConfigBuilder> {
        @Override
        protected ReaderGroupConfigBuilder newBuilder() {
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
            version(0).revision(2, this::write02, this::read02);
        }

        private void read00(RevisionDataInput revisionDataInput, ReaderGroupConfigBuilder builder) throws IOException {
            builder.automaticCheckpointIntervalMillis(revisionDataInput.readLong());
            builder.groupRefreshTimeMillis(revisionDataInput.readLong());
            ElementDeserializer<Stream> keyDeserializer = in -> Stream.of(in.readUTF());
            ElementDeserializer<StreamCut> valueDeserializer = in -> StreamCut.fromBytes(ByteBuffer.wrap(in.readArray()));
            builder.startFromStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.endingStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
        }

        private void write00(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(object.getAutomaticCheckpointIntervalMillis());
            revisionDataOutput.writeLong(object.getGroupRefreshTimeMillis());
            ElementSerializer<Stream> keySerializer = (out, s) -> out.writeUTF(s.getScopedName());
            ElementSerializer<StreamCut> valueSerializer = (out, cut) -> out.writeBuffer(new ByteArraySegment(cut.toBytes()));
            revisionDataOutput.writeMap(object.startingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeMap(object.endingStreamCuts, keySerializer, valueSerializer);
        }

        private void read01(RevisionDataInput revisionDataInput, ReaderGroupConfigBuilder builder) throws IOException {
            builder.automaticCheckpointIntervalMillis(revisionDataInput.readLong());
            builder.groupRefreshTimeMillis(revisionDataInput.readLong());
            ElementDeserializer<Stream> keyDeserializer = in -> Stream.of(in.readUTF());
            ElementDeserializer<StreamCut> valueDeserializer = in -> StreamCut.fromBytes(ByteBuffer.wrap(in.readArray()));
            builder.startFromStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.endingStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.maxOutstandingCheckpointRequest(revisionDataInput.readInt());
        }

        private void write01(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(object.getAutomaticCheckpointIntervalMillis());
            revisionDataOutput.writeLong(object.getGroupRefreshTimeMillis());
            ElementSerializer<Stream> keySerializer = (out, s) -> out.writeUTF(s.getScopedName());
            ElementSerializer<StreamCut> valueSerializer = (out, cut) -> out.writeBuffer(new ByteArraySegment(cut.toBytes()));
            revisionDataOutput.writeMap(object.startingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeMap(object.endingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeInt(object.getMaxOutstandingCheckpointRequest());
        }

        private void read02(RevisionDataInput revisionDataInput, ReaderGroupConfigBuilder builder) throws IOException {
            int ordinal = revisionDataInput.readCompactInt();
            builder.retentionType(StreamDataRetention.values()[ordinal]);
            builder.generation(revisionDataInput.readLong());
            builder.readerGroupId(revisionDataInput.readUUID());
        }

        private void write02(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactInt(object.retentionType.ordinal());
            revisionDataOutput.writeLong(object.getGeneration());
            revisionDataOutput.writeUUID(object.getReaderGroupId());
        }
    }

    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static ReaderGroupConfig fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }
}
