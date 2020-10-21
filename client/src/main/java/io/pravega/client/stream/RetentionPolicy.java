/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.io.Serializable;
import java.time.Duration;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class RetentionPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum RetentionType {
        /**
         * Set retention based on how long data has been in the stream in milliseconds.
         */
        TIME,

        /**
         * Set retention based on the total size of the data in the stream in bytes.
         */
        SIZE,

        /**
         * Set retention policy based on Consumption. 
         */
        CONSUMPTION
    }

    private final RetentionType retentionType;
    private final long retentionParam;
    private final ConsumptionLimits consumptionLimits;
    /**
     * Create a retention policy to configure a stream to periodically truncated
     * according to the specified duration.
     *
     * @param duration Period to retain data in a stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy byTime(Duration duration) {
        return RetentionPolicy.builder().retentionType(RetentionType.TIME).retentionParam(duration.toMillis()).build();
    }

    /**
     * Create a retention policy to configure a stream to truncate a stream
     * according to the amount of data currently stored.
     *
     * @param size Amount of data to retain in a stream.
     * @return Retention policy object.
     */
    public static RetentionPolicy bySizeBytes(long size) {
        return RetentionPolicy.builder().retentionType(RetentionType.SIZE).retentionParam(size).build();
    }
    
    /**
     * Create a retention policy to configure a stream to truncate a stream
     * according to positions of subscribed readergroups.
     *
     * @param limitByTime limit by time
     * @param minLimit min limit
     * @param maxLimit max limit
     * @return Retention policy object.
     */
    public static RetentionPolicy ofConsumption(boolean limitByTime, long minLimit, long maxLimit) {
        ConsumptionLimits.Type type = limitByTime ? ConsumptionLimits.Type.TIME : ConsumptionLimits.Type.SIZE;
        return RetentionPolicy.builder().retentionType(RetentionType.CONSUMPTION)
                              .consumptionLimits(new ConsumptionLimits(type, minLimit, maxLimit)).build();
    }
    
    @Data
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ConsumptionLimits {
        public enum Type {
            TIME,
            SIZE
        }

        private final Type type;
        private final long minValue;
        private final long maxValue;
    }
}
