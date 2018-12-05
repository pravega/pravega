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
    /*
     * The transaction timeout parameter corresponds to the lease renewal period.
     * In every period, the client must send at least one ping to keep the txn alive.
     * If the client fails to do so, then Pravega aborts the txn automatically. The client
     * sends pings internally and requires no application intervention, only that it sets
     * this parameter accordingly.
     *
     * This parameter is additionally used to determine the total amount of time that
     * a txn can remain open. Currently, we set the maximum amount of time for a
     * txn to remain open to be the minimum between 1 day and 1,000 times the value
     * of the lease renewal period. The 1,000 is hardcoded and has been chosen arbitrarily
     * to be a large enough value.
     *
     * The maximum allowed lease time by default is 120s, see:
     *
     * controller/src/main/resources/reference.conf
     *
     * The maximum allowed lease time is a configuration parameter of the controller
     * and can be changed accordingly. Note that being a controller-wide parameter,
     * it affects all transactions.
     */
    private final long transactionTimeoutTime;
    
    /**
     * The duration after the last call to {@link EventStreamWriter#noteTime(long)} which the
     * timestamp should be considered valid before it is forgotten. Meaning that after this long of
     * not calling {@link EventStreamWriter#noteTime(long)} readers that call
     * {@link EventStreamReader#getCurrentTimeWindow()} will receive a `null` when they are at the
     * corresponding position in the stream.
     */
    private final long timestampAggrigationTimeout;
    
    /**
     * Automatically invoke {@link EventStreamWriter#noteTime(long)} passing
     * {@link System#currentTimeMillis()} on a regular interval.
     */
    private final boolean automaticallyNoteTime;

    public static final class EventWriterConfigBuilder {
        private int initalBackoffMillis = 1;
        private int maxBackoffMillis = 20000;
        private int retryAttempts = 10;
        private int backoffMultiple = 10;
        private long transactionTimeoutTime = 30 * 1000 - 1;
        private long timestampAggrigationTimeout = 120 * 1000;
        private boolean automaticallyNoteTime = false; 
    }
}
