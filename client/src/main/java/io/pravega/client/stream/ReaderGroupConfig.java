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
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Singular;

import static com.google.common.base.Preconditions.checkArgument;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

   private final long groupRefreshTimeMillis;
   @Getter
   private final long automaticCheckpointIntervalMillis;

   @Singular("stream")
   private final Map<String, StreamCut> startingStreamCuts;


   public static ReaderGroupConfigBuilder builder() {
              return new ReaderGroupConfigBuilderWithValidation();
   }

   public static class ReaderGroupConfigBuilder {
       private long groupRefreshTimeMillis = 3000;
       private long automaticCheckpointIntervalMillis = 120000;

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


       public ReaderGroupConfigBuilder startFromStreamCut(final Map<Stream, StreamCut> streamCuts) {
           this.startingStreamCuts(streamCuts.entrySet().stream()
                                             .collect(Collectors.toMap(e -> e.getKey().getStreamName(), Map.Entry::getValue)));
           return this;
       }

       public ReaderGroupConfigBuilder startFromCheckpoint(final Checkpoint checkpoint) {
           this.startingStreamCuts(checkpoint.asImpl().getPositions().entrySet().stream()
                                             .collect(Collectors.toMap(e -> e.getKey().getStreamName(), Map.Entry::getValue)));
           return this;
       }

   }

   private static final class ReaderGroupConfigBuilderWithValidation extends ReaderGroupConfigBuilder {
       @Override
       public ReaderGroupConfig build() {
           checkArgument(super.startingStreamCuts$key != null && super.startingStreamCuts$key.size() > 0,
                   "Stream names that the reader group can read from cannot be empty");

           IntStream.range(0, super.startingStreamCuts$key.size())
                    .forEach(index ->
                            checkArgument(super.startingStreamCuts$key.get(index).equals(
                                    super.startingStreamCuts$value.get(index).asImpl().getStream().getStreamName())));
           return super.build();
       }

   }

   public void checkArguments() {
       checkArgument(startingStreamCuts.size() > 0,
               "Stream names that the reader group can read from cannot be empty");

       startingStreamCuts.entrySet().forEach(e ->
               checkArgument(e.getKey().equals(e.getValue().asImpl().getStream().getStreamName()),
                       "Cannot have StreamCuts of a different Stream"));
   }
}
