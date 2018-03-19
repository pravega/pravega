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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkArgument;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

   private final long groupRefreshTimeMillis;
   @Getter
   private final long automaticCheckpointIntervalMillis;

   private final Map<Stream, StreamCut> startingStreamCuts;

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
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        * @param scopedStreamName Scoped Name of the Stream.
        * @param startStreamCut Start {@link StreamCut}
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scopedStreamName, final StreamCut startStreamCut) {
           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(Stream.of(scopedStreamName), startStreamCut);
           return this;
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        * @param scopedStreamName Stream name.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String scopedStreamName) {
           return stream(scopedStreamName, StreamCut.UNBOUNDED);
       }

       /**
        * Add a stream and its associated start {@link StreamCut} to be read by the readers of a ReaderGroup.
        * @param stream Stream.
        * @param startStreamCut Start {@link StreamCut}
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream, final StreamCut startStreamCut) {
           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(stream, startStreamCut);
           return this;
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        * @param stream Stream.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final Stream stream) {
           return stream(stream, StreamCut.UNBOUNDED);
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

           startingStreamCuts.forEach((s, streamCut) -> {
               if (!streamCut.equals(StreamCut.UNBOUNDED)) {
                   checkArgument(s.equals(streamCut.asImpl().getStream()));
               }
           });
           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis, startingStreamCuts);
       }
   }
}
