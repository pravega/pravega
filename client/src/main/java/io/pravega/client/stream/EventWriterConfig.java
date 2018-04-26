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

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventWriterConfig implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private final int initalBackoffMillis;
    private final int maxBackoffMillis;
    private final int retryAttempts;
    private final int backoffMultiple;
    /**
     * The maximum amount of time, in milliseconds, which a transaction can run before it is considered failed.
     */
    private final long transactionTimeoutTime;
    /**
     * The maximum amount of time, in milliseconds after a scale operation has been initiated before a transaction is timed out. 
     */
    private final long transactionTimeoutScaleGracePeriod;
    
    public static final class EventWriterConfigBuilder {
        private int initalBackoffMillis = 1;
        private int maxBackoffMillis = 20000;
        private int retryAttempts = 10;
        private int backoffMultiple = 10;
        private long transactionTimeoutTime = 30 * 1000 - 1;
        private long transactionTimeoutScaleGracePeriod = -1;
    }
    
    
    public long getTransactionTimeoutScaleGracePeriod() {
        if (transactionTimeoutScaleGracePeriod < 0) {
            return transactionTimeoutTime;
        }
        return transactionTimeoutScaleGracePeriod;  
    }
}
