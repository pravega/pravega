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
       public ReaderGroupConfigBuilder startingTime(long time) {
           startingPosition = Sequence.create(time, 0);
           return this;
       }
   }

}
