/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

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

    public static final class EventWriterConfigBuilder {
        private int initalBackoffMillis = 1;
        private int maxBackoffMillis = 60000;
        private int retryAttempts = 5;
        private int backoffMultiple = 10;
    }
    
}
