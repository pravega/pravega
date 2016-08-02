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

package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.storage.DataLogInitializationException;
import com.emc.logservice.storage.DataLogNotAvailableException;
import com.emc.logservice.storage.DurableDataLogException;
import com.emc.nautilus.common.Exceptions;
import com.emc.nautilus.common.LoggerHelpers;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * General client for DistributedLog.
 */
@Slf4j
class LogClient implements AutoCloseable {
    //region Members

    private static final String DISTRIBUTED_LOG_URI_FORMAT = "distributedlog://%s:%d/%s";
    private final DistributedLogConfig config;
    private final HashMap<String, LogHandle> handles;
    private final String clientId;
    private final URI namespaceUri;
    private final String traceObjectId;
    private DistributedLogNamespace namespace;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogClient class.
     *
     * @param clientId The Id of this client.
     * @param config   The configuration for this LogClient.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the clientId is invalid.
     */
    public LogClient(String clientId, DistributedLogConfig config) {
        Preconditions.checkNotNull(config, "config");
        Exceptions.checkNotNullOrEmpty(clientId, "clientId");

        this.clientId = clientId;
        this.config = config;
        this.handles = new HashMap<>();
        String rawUri = String.format(DISTRIBUTED_LOG_URI_FORMAT, config.getDistributedLogHost(), config.getDistributedLogPort(), config.getDistributedLogNamespace());
        this.namespaceUri = URI.create(rawUri);
        this.traceObjectId = String.format("%s#%s", rawUri, this.clientId);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "close", this.closed);
        if (!this.closed) {
            ArrayList<LogHandle> handlesToClose;
            synchronized (this.handles) {
                handlesToClose = new ArrayList<>(this.handles.values());
                this.handles.clear();
            }

            for (LogHandle handle : handlesToClose) {
                try {
                    handle.close();
                } catch (Exception ex) {
                    log.error("{}: Unable to close handle for '{}'. {}", this.traceObjectId, handle == null ? "(null)" : handle.getLogName(), ex);
                }
            }

            if (this.namespace != null) {
                this.namespace.close();
                log.info("{}: Closed DistributedLog Namespace.", this.traceObjectId);
            }

            this.closed = true;
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "close", traceId);
    }

    //endregion

    //region Operations

    /**
     * Initializes the LogClient.
     *
     * @throws ObjectClosedException   If the LogClient is closed.
     * @throws IllegalStateException   If the LogClient is already initialized.
     * @throws DurableDataLogException If an exception is thrown during initialization. The actual exception thrown may
     *                                 be a derived exception from this one, which provides more information about
     *                                 the failure reason.
     */
    public void initialize() throws DurableDataLogException {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "initialize");
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.namespace == null, "LogClient is already initialized.");

        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setImmediateFlushEnabled(true)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
                .setCreateStreamIfNotExists(true);

        try {
            this.namespace = DistributedLogNamespaceBuilder
                    .newBuilder()
                    .conf(conf)
                    .uri(this.namespaceUri)
                    .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                    .clientId(this.clientId)
                    .build();
            log.info("{} Opened DistributedLog Namespace.", this.traceObjectId);
        } catch (IllegalArgumentException | NullPointerException ex) {
            //configuration issue
            throw new DataLogInitializationException("Unable to create a DistributedLog Namespace. DistributedLog reports bad configuration.", ex);
        } catch (IOException ex) {
            // Namespace not available, ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to access DistributedLog Namespace.", ex);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "initialize", traceId);
    }

    /**
     * Gets (or creates) a LogHandle for a particular log name.
     *
     * @param logName The name of the log.
     * @return A CompletableFuture that, when completed, will contain the requested result. If the operation failed,
     * the Future will contain the exception that caused the failure. All Log-related exceptions will inherit from the
     * DurableDataLogException class.
     */
    public LogHandle getLogHandle(String logName) throws DurableDataLogException {
        LogHandle handle;
        boolean newHandle = false;
        synchronized (this.handles) {
            // Try to get an existing handle.
            handle = this.handles.getOrDefault(logName, null);

            // If no such thing, create a new one and return the Future for it. As such, if we get concurrent requests
            // for the same log id, we will be not trying to create multiple such handles.
            if (handle == null) {
                newHandle = true;
                handle = new LogHandle(logName, this::handleLogHandleClosed);
                try {
                    this.handles.put(logName, handle);
                    handle.initialize(this.namespace);
                } catch (Exception ex) {
                    handle.close();
                    throw ex;
                }
            }
        }

        if (newHandle) {
            log.trace("{} Registered handle for '{}'.", this.traceObjectId, logName);
        }

        return handle;
    }

    private void handleLogHandleClosed(LogHandle handle) {
        if (handle == null) {
            return;
        }

        unregisterLogHandle(handle.getLogName());
    }

    private void unregisterLogHandle(String logName) {
        boolean removed;
        synchronized (this.handles) {
            removed = this.handles.remove(logName) != null;
        }

        if (removed) {
            log.trace("{} Unregistered handle for '{}'.", this.traceObjectId, logName);
        }
    }

    //endregion
}
