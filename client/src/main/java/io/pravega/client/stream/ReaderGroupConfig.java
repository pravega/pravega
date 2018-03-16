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

import java.util.stream.Collectors;
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

   private final Map<String, StreamCut> startingStreamCuts;

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
        * @param streamName Stream name.
        * @param startStreamCut Start {@link StreamCut}
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String streamName, final StreamCut startStreamCut) {
           if (startingStreamCuts == null) {
               startingStreamCuts = new HashMap<>();
           }
           this.startingStreamCuts.put(streamName, startStreamCut);
           return this;
       }

       /**
        * Add a stream that needs to be read by the readers of a ReaderGroup. The current starting position of the stream
        * will be used as the starting StreamCut.
        * @param streamName Stream name.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder stream(final String streamName) {
           return stream(streamName, StreamCut.UNBOUNDED);
       }

       /**
        * Ensure the readers of the ReaderGroup start from this provided streamCuts.
        * @param streamCuts Map of {@link Stream} and its corresponding {@link StreamCut}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromStreamCut(final Map<Stream, StreamCut> streamCuts) {
           this.startingStreamCuts(streamCuts.entrySet().stream()
                                             .collect(Collectors.toMap(e -> e.getKey().getStreamName(), Map.Entry::getValue)));
           return this;
       }

       /**
        * Ensure the readers of the ReaderGroup start from the provided {@link Checkpoint}
        * @param checkpoint {@link Checkpoint}.
        * @return Reader group config builder.
        */
       public ReaderGroupConfigBuilder startFromCheckpoint(final Checkpoint checkpoint) {
           this.startingStreamCuts(checkpoint.asImpl().getPositions().entrySet().stream()
                                             .collect(Collectors.toMap(e -> e.getKey().getStreamName(), Map.Entry::getValue)));
           return this;
       }

       public ReaderGroupConfig build() {
           checkArgument(startingStreamCuts != null && startingStreamCuts.size() > 0,
                   "Stream names that the reader group can read from cannot be empty");

           startingStreamCuts.forEach((s, streamCut) -> {
               if (!streamCut.equals(StreamCut.UNBOUNDED)) {
                   checkArgument(s.equals(streamCut.asImpl().getStream().getStreamName()));
               }
           });
           return new ReaderGroupConfig(groupRefreshTimeMillis, automaticCheckpointIntervalMillis, startingStreamCuts);
       }
   }
}
