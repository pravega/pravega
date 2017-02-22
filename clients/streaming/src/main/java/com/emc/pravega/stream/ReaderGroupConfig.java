/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@EqualsAndHashCode
public class ReaderGroupConfig {
   @Getter
   private final Sequence startingPosition;
   private final long groupRefreshTimeMillis;

   public static class ReaderGroupConfigBuilder {

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
