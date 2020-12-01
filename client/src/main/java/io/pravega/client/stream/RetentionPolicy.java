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

import com.google.common.base.Preconditions;
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
         * Also see: {@link ReaderGroupConfig.StreamDataRetention}
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
     * according to positions of subscribed reader groups.
     *
     * Provide null values for min and max if either limit is not desired.  
     * @param type Type of consumption limit which is one of time or size. 
     * @param minLimit min limit
     * @param maxLimit max limit
     * @return Retention policy object.
     */
    public static RetentionPolicy byConsumption(ConsumptionLimits.Type type, Long minLimit, Long maxLimit) {
        Preconditions.checkArgument(minLimit >= 0, "minLimit should be greater than 0");
        Preconditions.checkArgument(maxLimit >= minLimit, "maxLimit should be greater than minLimit");
        return RetentionPolicy.builder().retentionType(RetentionType.CONSUMPTION)
                              .consumptionLimits(ConsumptionLimits.builder().type(type)
                                                                  .minValue(minLimit)
                                                                  .maxValue(maxLimit)
                                                                  .build()).build();
    }
    
    @Data
    @Builder
    public static class ConsumptionLimits {
        public enum Type {
            TIME_MILLIS,
            SIZE_BYTES
        }

        private final Type type;
        private final long minValue;
        private final long maxValue;

        private ConsumptionLimits(Type type, Long minValue, Long maxValue) {
            this.type = type;
            this.minValue = minValue != null ? minValue : 0L;
            this.maxValue = maxValue != null ? maxValue : Long.MAX_VALUE;
        }
    }
}
