/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import java.util.UUID;

import com.emc.pravega.stream.StreamManager;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;

/**
 * The analog of the {@link StreamManager} for segments.
 * The implementation of this class will connect to a store & manage TCP connections.
 */
public interface SegmentManager {

    /**
     * Creates a new segment with given name. Returns false if the segment already
     * existed
     */
    boolean createSegment(String name);

    /**
     * Creates a new transaction on the specified segment
     */
    void createTransaction(String segmentName, UUID txId, long timeout);

    /**
     * Commits a transaction.
     */
    void commitTransaction(UUID txId) throws TxFailedException;

    /**
     * Drops a transaction (and all data written to it)
     */
    boolean dropTransaction(UUID txId);

    Transaction.Status checkTransactionStatus(UUID txId);
    
    /**
     * Opens a transaction for Appending to a segment.
     */
    SegmentOutputStream openTransactionForAppending(String segmentName, UUID txId);

    /**
     * Opens an existing segment for appending. this operation will fail if the segment does not
     * exist
     * This operation may be called multiple times on the same segment from the
     * same or different clients (i.e., there can be concurrent Stream Writers
     * in the same process space).
     */
    SegmentOutputStream openSegmentForAppending(String name, SegmentOutputConfiguration config);

    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     */
    SegmentInputStream openSegmentForReading(String name, SegmentInputConfiguration config);
}