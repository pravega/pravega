package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.*;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * General client for DistributedLog.
 */
@Slf4j
class LogClient implements AutoCloseable {
    //region Members

    private static final String DistributedLogUriFormat = "distributedlog://%s:%d/%s";
    private final DistributedLogConfig config;
    private final HashMap<String, CompletableFuture<LogHandle>> handles;
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
        String rawUri = String.format(DistributedLogUriFormat, config.getDistributedLogHost(), config.getDistributedLogPort(), config.getDistributedLogNamespace());
        this.namespaceUri = URI.create(rawUri);
        this.traceObjectId = String.format("%s#%s", rawUri, this.clientId);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "close", this.closed);
        if (!this.closed) {
            ArrayList<CompletableFuture<LogHandle>> handlesToClose;
            synchronized (this.handles) {
                handlesToClose = new ArrayList<>(this.handles.values());
                this.handles.clear();
            }

            for (CompletableFuture<LogHandle> cf : handlesToClose) {
                if (cf.isDone() && !cf.isCompletedExceptionally()) {
                    LogHandle handle = null;
                    try {
                        handle = cf.join();
                        handle.close();
                    }
                    catch (Exception ex) {
                        String handleId = handle == null ? "(null)" : handle.getLogName();
                        log.error("{}: Unable to close handle for '{}'. {}", this.traceObjectId, handleId, ex);
                    }
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
        }
        catch (IllegalArgumentException | NullPointerException ex) {
            //configuration issue
            throw new DataLogInitializationException("Unable to create a DistributedLog Namespace. DistributedLog reports bad configuration.", ex);
        }
        catch (IOException ex) {
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
    public CompletableFuture<LogHandle> getLogHandle(String logName) {
        CompletableFuture<LogHandle> handle;
        boolean newHandle = false;
        synchronized (this.handles) {
            // Try to get an existing handle.
            handle = this.handles.getOrDefault(logName, null);

            // If no such thing, create a new one and return the Future for it. As such, if we get concurrent requests
            // for the same log id, we will be not trying to create multiple such handles.
            if (handle == null) {
                handle = createLogHandle(logName);
                this.handles.put(logName, handle);
                newHandle = true;
            }
        }

        if (newHandle) {
            log.trace("{} Registered handle for '{}'.", this.traceObjectId, logName);

            // If we are creating a new handle and it failed, remove the "bad handle" from the map.
            handle.whenComplete((result, ex) -> {
                if (ex != null) {
                    unregisterLogHandle(logName);
                }
            });
        }

        return handle;
    }

    private CompletableFuture<LogHandle> createLogHandle(String logId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create a new handle, and pass in a callback so we unregister the handle when it's closed.
                LogHandle handle = new LogHandle(logId, this::handleLogHandleClosed);
                handle.initialize(this.namespace);
                return handle;
            }
            catch (DurableDataLogException ex) {
                throw new CompletionException(ex);
            }
        });
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
