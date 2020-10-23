/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.ScheduledExecutorService;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.summarizingLong;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ReaderGroupConfigSerializer SERIALIZER = new ReaderGroupConfigSerializer();
    private final long groupRefreshTimeMillis;
    @Getter
    private final long automaticCheckpointIntervalMillis;

    private final Map<Stream, StreamCut> startingStreamCuts;
    private final Map<Stream, StreamCut> endingStreamCuts;

    private final int maxOutstandingCheckpointRequest;

    @Getter
    private final boolean isSubscriber;
    @Getter
    private final boolean autoTruncateAtLastCheckpoint;

   public static class ReaderGroupConfigBuilder implements ObjectBuilder<ReaderGroupConfig> {
       private long groupRefreshTimeMillis = 3000; //default value
       private long automaticCheckpointIntervalMillis = 30000; //default value
       // maximum outstanding checkpoint request that is allowed at any given time.
       private int maxOutstandingCheckpointRequest = 3; //default value
       private boolean subscribedForRetention = false; //default value
       private boolean autoPublishLastCheckpoint = false; //default value

       /**
        * Makes the reader group a subscriber reader group. During CBR (Consumption Based Retention),
        * the subscriber reader group's positions are considered during truncation. The stream will truncate
        * to the last checkpoint position of this group.
        *
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder isSubscribedForRetention() {
           this.subscribedForRetention = true;
           return this;
       }

       /**
        * Allows for every checkpoint initiated by this reader group (automatic or manual)
        * to be published as a truncation point to the controller, if the reader group is a subscriber reader group.
        *
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder autoPublishCheckpoint() {
           this.autoPublishLastCheckpoint = true;
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

           //basic check to make sure autoTruncateAtLastCheckpoint is not true while isSubscriber is false
           if (!subscribedForRetention) {
               Preconditions.checkArgument(!autoPublishLastCheckpoint,
                       "ReaderGroup should be subscriber to be able to auto-truncate");
           }

           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis,
                   startingStreamCuts, endingStreamCuts, maxOutstandingCheckpointRequest, subscribedForRetention, autoPublishLastCheckpoint);
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
            version(1).revision(0, this::write10, this::read10);
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

        private void read10(RevisionDataInput revisionDataInput, ReaderGroupConfigBuilder builder) throws IOException {
            builder.automaticCheckpointIntervalMillis(revisionDataInput.readLong());
            builder.groupRefreshTimeMillis(revisionDataInput.readLong());
            ElementDeserializer<Stream> keyDeserializer = in -> Stream.of(in.readUTF());
            ElementDeserializer<StreamCut> valueDeserializer = in -> StreamCut.fromBytes(ByteBuffer.wrap(in.readArray()));
            builder.startFromStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.endingStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.maxOutstandingCheckpointRequest(revisionDataInput.readInt());
            if (revisionDataInput.readBoolean()) {
                builder.isSubscribedForRetention();
            }
            if (revisionDataInput.readBoolean()) {
                builder.autoPublishCheckpoint();
            }
        }

        private void write10(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(object.getAutomaticCheckpointIntervalMillis());
            revisionDataOutput.writeLong(object.getGroupRefreshTimeMillis());
            ElementSerializer<Stream> keySerializer = (out, s) -> out.writeUTF(s.getScopedName());
            ElementSerializer<StreamCut> valueSerializer = (out, cut) -> out.writeBuffer(new ByteArraySegment(cut.toBytes()));
            revisionDataOutput.writeMap(object.startingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeMap(object.endingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeInt(object.getMaxOutstandingCheckpointRequest());
            revisionDataOutput.writeBoolean(object.isSubscriber());
            revisionDataOutput.writeBoolean(object.isAutoTruncateAtLastCheckpoint());
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
