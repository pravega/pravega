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
import java.util.concurrent.ScheduledExecutorService;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
public class ReaderGroupConfig implements Serializable {

   private final Sequence startingPosition;
   private final long groupRefreshTimeMillis;
   @Getter
   private final long automaticCheckpointIntervalMillis;   

   public static final class ReaderGroupConfigBuilder {
       private long groupRefreshTimeMillis = 3000;
       private long automaticCheckpointIntervalMillis = 120000;

       /**
         * Returns a config builder that started at a given time.
         *
         * @param time A time to create sequence at.
         * @return Reader group config builder.
         */
       public ReaderGroupConfigBuilder startingTime(long time) {
           startingPosition = Sequence.create(time, 0);
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
   }
}
