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
    private long highestCommittedSequenceNumber;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterState class.
     */
    WriterState() {
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        this.highestCommittedSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
    }

    //endregion

    //region Properties

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

    /**
     * Gets a value indicating the Sequence Number of the last Operation that was committed to Storage, having the
     * property that all Operations prior to it have also been successfully committed.
     *
     * @return The result.
     */
    long getHighestCommittedSequenceNumber() {
        return this.highestCommittedSequenceNumber;
    }

    /**
     * Sets the Sequence Number of the last Operation committed to Storage (with all prior Operations also committed).
     *
     * @param value The Sequence Number to set.
     */
    void setHighestCommittedSequenceNumber(long value) {
        Preconditions.checkArgument(value >= this.highestCommittedSequenceNumber, "New highestCommittedSequenceNumber cannot be smaller than the previous one.");
        Preconditions.checkArgument(value <= this.lastReadSequenceNumber, "New highestCommittedSequenceNumber cannot be larger than lastReadSequenceNumber.");
        this.highestCommittedSequenceNumber = value;
    }

    @Override
    public String toString() {
        return String.format("LastRead = %d, HighestCommitted = %d", this.lastReadSequenceNumber, this.highestCommittedSequenceNumber);
    }

    //endregion
}