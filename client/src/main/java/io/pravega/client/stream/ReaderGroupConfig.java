/*
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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@EqualsAndHashCode
public class ReaderGroupConfig implements Serializable {
   @Getter
   private final Sequence startingPosition;
   private final long groupRefreshTimeMillis;

   public static final class ReaderGroupConfigBuilder {

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
