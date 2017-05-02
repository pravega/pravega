/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@EqualsAndHashCode
public class ReaderGroupConfig implements Serializable {
   @Getter
   private final Sequence startingPosition;
   private final long groupRefreshTimeMillis;

   private final long automaticCheckpointIntervalSeconds;
   
   public static final class ReaderGroupConfigBuilder {
       long automaticCheckpointIntervalSeconds = 30;

       /**
         * Returns a config builder that started at  a given time.
         *
         * @param time A time to create sequence at.
         * @return Sequence instance.
         */
       public ReaderGroupConfigBuilder startingTime(long time) {
           startingPosition = Sequence.create(time, 0);
           return this;
       }
   }

}
