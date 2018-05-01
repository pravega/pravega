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
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.summarizingInt;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final long groupRefreshTimeMillis;
    @Getter
    private final long automaticCheckpointIntervalMillis;

    private final Map<Stream, StreamCut> startingStreamCuts;
    private final Map<Stream, StreamCut> endingStreamCuts;

   public static class ReaderGroupConfigBuilder {
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

           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis,
                   startingStreamCuts, endingStreamCuts);
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

       private void validateStreamCut(Map<Stream, StreamCut> streamCuts) {
           streamCuts.forEach((s, streamCut) -> {
               if (!streamCut.equals(StreamCut.UNBOUNDED)) {
                   checkArgument(s.equals(streamCut.asImpl().getStream()));
               }
           });
       }
   }
}
