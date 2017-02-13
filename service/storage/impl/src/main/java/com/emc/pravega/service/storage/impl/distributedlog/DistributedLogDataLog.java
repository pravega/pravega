/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Twitter DistributedLog implementation for DurableDataLog.
 */
class DistributedLogDataLog implements DurableDataLog {
    //region Members

    private final LogClient client;
    private final String logName;
    private LogHandle handle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogDataLog class.
     *
     * @param logName The name of the DistributedLog Log to use.
     * @param client  The DistributedLog Client to use.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If logName is an empty string.
     */
    DistributedLogDataLog(String logName, LogClient client) {
        Preconditions.checkNotNull(client, "client");
        Exceptions.checkNotNullOrEmpty(logName, "logName");

        this.logName = logName;
        this.client = client;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.handle != null) {
            this.handle.close();
        }
    }

    //endregion

    //region DurableDataLog Implementation

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        Preconditions.checkState(this.handle == null, "DistributedLogDataLog is already initialized.");
        this.handle = this.client.getLogHandle(this.logName);
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        ensureInitialized();
        return this.handle.append(data, timeout);
    }

    @Override
    public CompletableFuture<Boolean> truncate(LogAddress logAddress, Duration timeout) {
        ensureInitialized();
        Preconditions.checkArgument(logAddress instanceof DLSNAddress, "Invalid logAddress. Expected a DLSNAddress.");
        return this.handle.truncate((DLSNAddress) logAddress, timeout);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ensureInitialized();
        return this.handle.getReader(afterSequence);
    }

    @Override
    public int getMaxAppendLength() {
        ensureInitialized();
        return LogHandle.MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        ensureInitialized();
        return this.handle.getLastTransactionId();
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.handle != null, "DistributedLogDataLog is not initialized.");
    }

    //endregion
}
