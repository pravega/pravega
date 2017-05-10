/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.writer;

/**
 * Defines a general Processor for Operations.
 */
interface OperationProcessor {
    /**
     * Gets a value indicating whether the SegmentAggregator is closed (for any kind of operations).
     */
    boolean isClosed();

    /**
     * Gets the SequenceNumber of the first operation that is not fully committed to Storage.
     */
    long getLowestUncommittedSequenceNumber();
}
