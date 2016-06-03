package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.storageabstraction.*;
import com.twitter.distributedlog.DistributedLogConfiguration;
import com.twitter.distributedlog.DistributedLogConstants;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * General client for DistributedLog
 */
class LogClient implements AutoCloseable {
    //region Members

    private static final String DistributedLogUriFormat = "distributedlog://%s:%d/%s";
    private final DistributedLogConfig config;
    private final HashMap<String, CompletableFuture<LogHandle>> handles;
    private final String clientId;
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
        if (config == null) {
            throw new NullPointerException("config");
        }

        if (clientId == null) {
            throw new NullPointerException("clientId");
        }

        if (clientId.length() == 0) {
            throw new IllegalArgumentException("clientId");
        }

        this.clientId = clientId;
        this.config = config;
        this.handles = new HashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            ArrayList<CompletableFuture<LogHandle>> handlesToClose;
            synchronized (this.handles) {
                handlesToClose = new ArrayList<>(this.handles.values());
                this.handles.clear();
            }

            for (CompletableFuture<LogHandle> cf : handlesToClose) {
                if (cf.isDone() && !cf.isCompletedExceptionally()) {
                    try {
                        LogHandle handle = cf.join();
                        handle.close();
                    }
                    catch (Exception ex) {
                        System.err.println(ex);// TODO: fix.
                    }
                }
            }

            this.namespace.close();
            this.closed = true;
        }
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
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (this.namespace != null) {
            throw new IllegalStateException("LogClient is already initialized.");
        }

        URI uri = URI.create(String.format(DistributedLogUriFormat, config.getDistributedLogHost(), config.getDistributedLogPort(), config.getDistributedLogNamespace()));
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setImmediateFlushEnabled(true)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
                .setCreateStreamIfNotExists(true);

        try {
            this.namespace = DistributedLogNamespaceBuilder.newBuilder()
                                                           .conf(conf)
                                                           .uri(uri)
                                                           .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                                                           .clientId(this.clientId)
                                                           .build();
        }
        catch (IllegalArgumentException | NullPointerException ex) {
            //configuration issue
            throw new DataLogInitializationException("Unable to create a DistributedLog Namespace. DistributedLog reports bad configuration.", ex);
        }
        catch (IOException ex) {
            // Namespace not available, ZooKeeper not reachable, some other environment issue.
            throw new DataLogNotAvailableException("Unable to access DistributedLog Namespace.", ex);
        }
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
            // If we are creating a new handle and it failed, remove the "bad handle" from the map.
            handle.whenComplete((result, ex) -> {
                if (ex != null) {
                    synchronized (this.handles) {
                        this.handles.remove(logName);
                    }
                }
            });
        }

        return handle;
    }

    private CompletableFuture<LogHandle> createLogHandle(String logId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new LogHandle(this.namespace, logId);
            }
            catch (DurableDataLogException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    //endregion
}
