/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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

import java.util.stream.Collectors;

import io.pravega.shared.NameUtils;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.summarizingInt;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ReaderGroupConfigSerializer SERIALIZER = new ReaderGroupConfigSerializer();
    private static final String USE_DEFAULT_SCOPE = "_defaultScope";
    private final long groupRefreshTimeMillis;
    @Getter
    private final long automaticCheckpointIntervalMillis;

    private final String defaultScope;
    private final Map<String, StreamCut> startingStreamCuts;
    private final Map<String, StreamCut> endingStreamCuts;

   public static class ReaderGroupConfigBuilder implements ObjectBuilder<ReaderGroupConfig> {
       private long groupRefreshTimeMillis = 3000; //default value
       private long automaticCheckpointIntervalMillis = 120000; //default value

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
        * Sets the default scope to be used by the ReaderGroupConfig.
        *
        * @param scope Default Scope.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder defaultScope(final String scope) {
           this.defaultScope = NameUtils.validateScopeName(scope);
           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} and end {@link StreamCut} to be read by the
        * readers of a ReaderGroup.
        *
        * @param scope Scope of the stream.
        * @param streamName Stream name.
        * @param startStreamCut Start {@link StreamCut}.
        * @param endStreamCut End {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scope, final String streamName,
                                              final StreamCut startStreamCut, final StreamCut endStreamCut) {
           final String scopedStreamName = Stream.of(scope, streamName).getScopedName();

           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(scopedStreamName, startStreamCut);

           if (endingStreamCuts == null) {
               endingStreamCuts = new HashMap<>();
           }
           this.endingStreamCuts.put(scopedStreamName, endStreamCut);

           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} and end {@link StreamCut} to be read by the
        * readers of a ReaderGroup. {@link ReaderGroupConfig#getDefaultScope()} is used for the scope of the stream.
        *
        * @param streamName Stream name.
        * @param startStreamCut Start {@link StreamCut}.
        * @param endStreamCut End {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String streamName, final StreamCut startStreamCut, final StreamCut endStreamCut) {
           return stream(USE_DEFAULT_SCOPE, streamName, startStreamCut, endStreamCut);
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        *
        * @param scope Scope of the stream.
        * @param streamName Stream name.
        * @param startStreamCut Start {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scope, final String streamName, final StreamCut startStreamCut) {
           return stream(scope, streamName, startStreamCut, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        * {@link ReaderGroupConfig#getDefaultScope()} is used for the scope of the stream.
        *
        * @param streamName Stream name.
        * @param startStreamCut Start {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String streamName, final StreamCut startStreamCut) {
           return stream(USE_DEFAULT_SCOPE, streamName, startStreamCut, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        *
        * @param scope Scope of the stream.
        * @param streamName Stream name.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scope, final String streamName) {
           return stream(scope, streamName, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut. {@link ReaderGroupConfig#getDefaultScope()} is used for the scope of the stream.
        *
        * @param streamName Stream name.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String streamName) {
           return stream(USE_DEFAULT_SCOPE, streamName, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
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
           final String scopedStreamName = stream.getScopedName();
           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(scopedStreamName, startStreamCut);

           if (endingStreamCuts == null) {
               endingStreamCuts = new HashMap<>();
           }
           this.endingStreamCuts.put(scopedStreamName, endStreamCut);

           return this;
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        *
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
        *
        * @param stream Stream.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream) {
           return stream(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED);
       }

       /**
        * Ensure the readers of the ReaderGroup start from this provided streamCuts.
        *
        * @param streamCuts Map of {@link Stream} and its corresponding {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromStreamCuts(final Map<Stream, StreamCut> streamCuts) {
           this.startingStreamCuts(streamCuts.entrySet().stream()
                                             .collect(Collectors.toMap(o -> o.getKey().getScopedName(),
                                                     Map.Entry::getValue)));
           return this;
       }

       /**
        * Ensure the readers of the ReaderGroup start from the provided {@link Checkpoint}.
        *
        * @param checkpoint {@link Checkpoint}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromCheckpoint(final Checkpoint checkpoint) {
           this.startingStreamCuts(checkpoint.asImpl().getPositions().entrySet().stream()
                                             .collect(Collectors.toMap(o -> o.getKey().getScopedName(),
                                                     Map.Entry::getValue)));
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

           //fetch StreamCuts with the specified default scope.
           final Map<String, StreamCut> startCutsWithDefaultScope = getCutsWithDefaultScope(startingStreamCuts);
           final Map<String, StreamCut> endCutsWithDefaultScope = getCutsWithDefaultScope(endingStreamCuts);

           //validate start and end StreamCuts
           validateStreamCut(startCutsWithDefaultScope);
           validateStreamCut(endCutsWithDefaultScope);

           //basic check to verify if endStreamCut > startStreamCut.
           validateStartAndEndStreamCuts(startCutsWithDefaultScope, endCutsWithDefaultScope);

           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis, defaultScope,
                   startCutsWithDefaultScope, endCutsWithDefaultScope);
       }

        private Map<String, StreamCut> getCutsWithDefaultScope(final Map<String, StreamCut> streamCutMap) {
            return streamCutMap.entrySet().stream().collect(Collectors.toMap(e -> {
                Stream s = Stream.of(e.getKey());
                if (s.getScope().equals(USE_DEFAULT_SCOPE)) {
                    checkArgument(defaultScope != null, "defaultScope should be set");
                    return Stream.of(defaultScope, s.getStreamName()).getScopedName();
                } else {
                    return e.getKey();
                }
            }, Map.Entry::getValue));
        }

        private void validateStartAndEndStreamCuts(Map<String, StreamCut> startStreamCuts,
                                                  Map<String, StreamCut> endStreamCuts) {
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
                        .forEach(s -> Preconditions.checkArgument(startPositions.get(s) <= endPositions.get(s),
                                "Segment offset in startStreamCut should be <= segment offset in endStreamCut."));

           val fromSCSummary = startPositions.keySet()
                                             .stream().collect(summarizingInt(Segment::getSegmentNumber));
           val toSCSummary = endPositions.keySet()
                                         .stream().collect(summarizingInt(Segment::getSegmentNumber));
           Preconditions.checkArgument(fromSCSummary.getMin() <= toSCSummary.getMin(),
                   "Start stream cut must precede end stream cut.");
           Preconditions.checkArgument(fromSCSummary.getMax() <= toSCSummary.getMax(),
                   "Start stream cut must precede end stream cut.");
       }

       private void validateStreamCut(Map<String, StreamCut> streamCuts) {
           streamCuts.forEach((s, streamCut) -> {
               if (!streamCut.equals(StreamCut.UNBOUNDED)) {
                   checkArgument(s.equals(streamCut.asImpl().getStream().getScopedName()));
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
        }

        private void read00(RevisionDataInput revisionDataInput, ReaderGroupConfigBuilder builder) throws IOException {
            builder.automaticCheckpointIntervalMillis(revisionDataInput.readLong());
            builder.groupRefreshTimeMillis(revisionDataInput.readLong());
            ElementDeserializer<String> keyDeserializer = in -> in.readUTF();
            ElementDeserializer<StreamCut> valueDeserializer = in -> StreamCut.fromBytes(ByteBuffer.wrap(in.readArray()));
            builder.startingStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
            builder.endingStreamCuts(revisionDataInput.readMap(keyDeserializer, valueDeserializer));
        }

        private void write00(ReaderGroupConfig object, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(object.getAutomaticCheckpointIntervalMillis());
            revisionDataOutput.writeLong(object.getGroupRefreshTimeMillis());
            ElementSerializer<String> keySerializer = (out, s) -> out.writeUTF(s);
            ElementSerializer<StreamCut> valueSerializer = (out, cut) -> out.writeArray(new ByteArraySegment(cut.toBytes()));
            revisionDataOutput.writeMap(object.startingStreamCuts, keySerializer, valueSerializer);
            revisionDataOutput.writeMap(object.endingStreamCuts, keySerializer, valueSerializer);
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
