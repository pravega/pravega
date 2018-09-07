/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration for the TableWriter.
 */
@Data
@Builder
public class TableWriterConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int initialBackoffMillis;
    private final int maxBackoffMillis;
    private final int retryAttempts;
    private final int backoffMultiple;
    private final long transactionTimeoutTime;

    public static final class TableWriterConfigBuilder {
        private int initialBackoffMillis = 1;
        private int maxBackoffMillis = 20000;
        private int retryAttempts = 10;
        private int backoffMultiple = 10;
        private long transactionTimeoutTime = 30 * 1000 - 1;
    }
}
