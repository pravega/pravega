/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.writer;

import com.emc.pravega.service.server.logs.operations.Operation;
import com.google.common.base.Preconditions;

/**
 * Holds the current state for the StorageWriter.
 */
class WriterState {
    //region Members

    private long lastReadSequenceNumber;
    private long lastTruncatedSequenceNumber;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterState class.
     */
    WriterState() {
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        this.lastTruncatedSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Sequence Number of the last Truncated Operation.
     *
     * @return The result.
     */
    long getLastTruncatedSequenceNumber() {
        return this.lastTruncatedSequenceNumber;
    }

    /**
     * Sets the Sequence Number of the last Truncated Operation.
     *
     * @param value The Sequence Number to set.
     */
    void setLastTruncatedSequenceNumber(long value) {
        Preconditions.checkArgument(value >= this.lastTruncatedSequenceNumber, "New LastTruncatedSequenceNumber cannot be smaller than the previous one.");
        this.lastTruncatedSequenceNumber = value;
    }

    /**
     * Gets a value indicating the Sequence Number of the last read Operation (from the Operation Log).
     *
     * @return The result.
     */
    long getLastReadSequenceNumber() {
        return this.lastReadSequenceNumber;
    }

    /**
     * Sets the Sequence Number of the last read Operation.
     *
     * @param value The Sequence Number to set.
     */
    void setLastReadSequenceNumber(long value) {
        Preconditions.checkArgument(value >= this.lastReadSequenceNumber, "New LastReadSequenceNumber cannot be smaller than the previous one.");
        this.lastReadSequenceNumber = value;
    }

    @Override
    public String toString() {
        return String.format("LastRead = %d", this.lastReadSequenceNumber);
    }

    //endregion
}