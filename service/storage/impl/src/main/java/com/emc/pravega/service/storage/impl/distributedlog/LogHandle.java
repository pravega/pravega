/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.SegmentStoreMetricsNames;
import com.emc.pravega.common.Timer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.metrics.Counter;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.OpStatsLogger;
import com.emc.pravega.common.metrics.StatsLogger;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DataLogInitializationException;
import com.emc.pravega.service.storage.DataLogNotAvailableException;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.service.storage.WriteFailureException;
import com.emc.pravega.service.storage.WriteTooLongException;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.AsyncLogWriter;
import com.twitter.distributedlog.DLSN;
import com.twitter.distributedlog.DistributedLogManager;
import com.twitter.distributedlog.LogReader;
import com.twitter.distributedlog.LogRecord;
import com.twitter.distributedlog.LogRecordWithDLSN;
import com.twitter.distributedlog.exceptions.BKTransmitException;
import com.twitter.distributedlog.exceptions.LockingException;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.LogRecordTooLongException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.exceptions.StreamNotReadyException;
import com.twitter.distributedlog.exceptions.WriteCancelledException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Read/Write handle for a particular DistributedLog Stream.
 */
@Slf4j
class LogHandle implements AutoCloseable {
    //region Members
    /**
     * Maximum append length, as specified by DistributedLog (this is hardcoded inside DLog's code).
     */
    static final int MAX_APPEND_LENGTH = 1024 * 1024 - 8 * 1024 - 20;
    static final long START_TRANSACTION_ID = 0;

    private final AtomicLong lastTransactionId;
    private final String logName;
    private final HashSet<DistributedLogReader> activeReaders;
    private final Consumer<LogHandle> handleClosedCallback;
    private DistributedLogManager logManager;
    private AsyncLogWriter logWriter;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LogHandle class.
     *
     * @param logName              The Distributed Log Name to bind to.
     * @param handleClosedCallback A callback that will be invoked when this LogHandle is closed.
     */
    LogHandle(String logName, Consumer<LogHandle> handleClosedCallback) {
        Preconditions.checkNotNull(handleClosedCallback, "handleClosedCallback");
        Exceptions.checkNotNullOrEmpty(logName, "logName");

        this.logName = logName;
        this.handleClosedCallback = handleClosedCallback;
        this.lastTransactionId = new AtomicLong(START_TRANSACTION_ID);
        this.activeReaders = new HashSet<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            try {
                // Check for null for each of these components since we may be attempting to close them from the constructor
                // due to an initialization failure.
                if (this.logWriter != null) {
                    FutureUtils.result(this.logWriter.asyncClose());
                    log.debug("{}: Closed LogWriter.", this.logName);
                }

                if (this.logManager != null) {
                    FutureUtils.result(this.logManager.asyncClose());
                    log.debug("{}: Closed LogManager.", this.logName);
                }
            } catch (IOException ex) {
                //TODO: retry policy.
                log.error("{}: Unable to close LogHandle. {}", this.logName, ex);
            }

            this.closed = true;

            // Close all active readers.
            List<DistributedLogReader> readersToClose;
            synchronized (this.activeReaders) {
                readersToClose = new ArrayList<>(this.activeReaders);
            }

            readersToClose.forEach(DistributedLogReader::close);
            assert this.activeReaders.size() == 0 : "Not all readers were closed.";

            log.info("{}: Closed (LastTransactionId = {}).", this.logName, this.lastTransactionId);
            CallbackHelpers.invokeSafely(this.handleClosedCallback, this, null);
        }
    }

    //endregion

    //region Operations

    /**
     * Initializes the LogHandle and binds it to a Stream in the given DistributedLogNamespace.
     *
     * @param namespace The DistributedLog Namespace to bind to.
     * @throws DataLogInitializationException   If a general initialization error occurred.
     * @throws DataLogWriterNotPrimaryException If this client lost its exclusive write privileges for this log.
     * @throws DataLogNotAvailableException     If the DurableDataLog cannot be reached.
     * @throws DurableDataLogException          General error.
     */
    void initialize(DistributedLogNamespace namespace) throws DurableDataLogException {
        Preconditions.checkNotNull(namespace, "namespace");
        Preconditions.checkState(this.logManager == null, "LogHandle is already initialized.");
        final long traceId = LoggerHelpers.traceEnter(log, this.logName, "initialize");

        // Initialize Log Manager and Log Writer.
        boolean success = false;
        try {
            this.logManager = namespace.openLog(logName);
            log.debug("{}: Opened LogManager.", this.logName);

            this.logWriter = this.logManager.startAsyncLogSegmentNonPartitioned();
            log.debug("{}: Opened LogWriter.", this.logName);
            success = true;
        } catch (OwnershipAcquireFailedException ex) {
            // This means one of two things:
            // 1. Someone else currently holds the exclusive write lock.
            // 2. Someone else held the exclusive write lock, crashed, and ZooKeeper did not figure it out yet (due to a long Session Timeout).
            throw new DataLogWriterNotPrimaryException(String.format("Unable to acquire exclusive Writer for log '%s'.", logName), ex);
        } catch (IOException ex) {
            // Log does not exist or some other issue happened. Note that LogNotFoundException inherits from IOException, so it's also handled here.
            throw new DataLogNotAvailableException(String.format("Unable to create DistributedLogManager for log '%s'.", logName), ex);
        } catch (Exception ex) {
            // General exception, configuration issue, etc.
            throw new DataLogInitializationException(String.format("Unable to create DistributedLogManager for log '%s'.", logName), ex);
        } finally {
            if (!success) {
                try {
                    close();
                } catch (Exception ex) {
                    log.error("Unable to cleanup resources after the failed attempt to create a LogHandle for '{}'. {}", logName, ex);
                }
            }
        }

        try {
            this.lastTransactionId.set(this.logManager.getLastTxId());
        } catch (LogEmptyException ex) {
            this.lastTransactionId.set(START_TRANSACTION_ID);
        } catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to determine last transaction Id for log '%s'.", logName), ex);
        }

        log.info("{}: Initialized (LastTransactionId = {}).", this.logName, this.lastTransactionId);
        LoggerHelpers.traceLeave(log, this.logName, "initialize", traceId);
    }

    /**
     * Gets a value indicating the DistributedLog LogName for this Handle.
     */
    String getLogName() {
        return this.logName;
    }

    /**
     * Gets a value indicating the Id of the Last Transaction that was processed through this Handle/Log. If the Log is
     * currently empty, its value is 0.
     */
    long getLastTransactionId() {
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");
        return this.lastTransactionId.get();
    }

    /**
     * Appends the given data at the end of the Log.
     *
     * @param data    An InputStream representing the data to append.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the TransactionId of the append. If the append
     * failed, it will contain the exception that caused it to fail.
     */
    CompletableFuture<LogAddress> append(InputStream data, java.time.Duration timeout) {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");
        Preconditions.checkNotNull(data, "data");
        final long traceId = LoggerHelpers.traceEnter(log, this.logName, "append");

        final long transactionId = this.lastTransactionId.incrementAndGet();

        // This extra buffer allocation + copy is necessary because DL Writer only accepts entire byte[] arrays
        // (it doesn't provide an API for InputStream or byte[] + offset + length).
        byte[] buffer;
        try {
            int dataLength = data.available();
            if (dataLength > MAX_APPEND_LENGTH) {
                return FutureHelpers.failedFuture(new WriteTooLongException(dataLength, MAX_APPEND_LENGTH));
            }

            buffer = new byte[dataLength];
            int bytesRead = StreamHelpers.readAll(data, buffer, 0, buffer.length);
            assert bytesRead == buffer.length : String.format("StreamHelpers.ReadAll did not read entire input stream. Expected %d, Actual %d.", buffer.length, bytesRead);
        } catch (IOException ex) {
            return FutureHelpers.failedFuture(ex);
        }

        // Send the write to DistributedLog.
        log.debug("{}: LogWriter.write (TransactionId = {}, Length = {}).", this.logName, transactionId, buffer.length);
        Timer timer = new Timer();
        Future<DLSN> writeFuture = this.logWriter.write(new LogRecord(transactionId, buffer));
        CompletableFuture<LogAddress> result = toCompletableFuture(writeFuture, dlsn -> new DLSNAddress(transactionId, dlsn));
        result.thenRunAsync(() -> {
            Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
            Metrics.WRITE_BYTES.add(buffer.length);
        });
        if (log.isTraceEnabled()) {
            result = result.thenApply(r -> {
                LoggerHelpers.traceLeave(log, this.logName, "append", traceId, transactionId, buffer.length);
                return r;
            });
        }

        return result;
    }

    /**
     * Creates a new Reader starting after the given Transaction Id.
     *
     * @param afterTransactionId The Transaction Id right before where to start.
     * @return A CloseableIterator that returns DurableDataLog.ReadItems, each corresponding to an entry in the DurableLog Read.
     * @throws DurableDataLogException If the reader could not be opened.
     */
    CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> getReader(long afterTransactionId) throws DurableDataLogException {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");
        final long traceId = LoggerHelpers.traceEnter(log, this.logName, "getReader", afterTransactionId);

        DistributedLogReader reader;
        try {
            reader = new DistributedLogReader(afterTransactionId, this.logManager, this::unregisterReader);
        } catch (IOException ex) {
            throw new DurableDataLogException("Could not open reader.", ex);
        }

        // Keep track of the newly created reader.
        synchronized (this.activeReaders) {
            this.activeReaders.add(reader);
        }

        LoggerHelpers.traceLeave(log, this.logName, "getReader", traceId, reader.traceObjectId);
        return reader;
    }

    /**
     * Truncates the DistributedLog log up to (and including) the given DLSNAddress.
     *
     * @param upToAddress The DLSNAddress to truncate to.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the outcome of the operation. If the operation failed,
     * the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Boolean> truncate(DLSNAddress upToAddress, java.time.Duration timeout) {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");
        final long traceId = LoggerHelpers.traceEnter(log, this.logName, "truncate");

        log.info("{}: Truncate (LogAddress = {}).", this.logName, upToAddress);
        Future<Boolean> truncateFuture = this.logWriter.truncate(upToAddress.getDLSN());
        CompletableFuture<Boolean> result = toCompletableFuture(truncateFuture, b -> b);
        if (log.isTraceEnabled()) {
            result = result.thenApply(r -> {
                LoggerHelpers.traceLeave(log, this.logName, "truncate", traceId);
                return r;
            });
        }

        return result;
    }

    @Override
    public String toString() {
        return this.logName;
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
    }

    private void unregisterReader(DistributedLogReader reader) {
        boolean removed;
        synchronized (this.activeReaders) {
            removed = this.activeReaders.remove(reader);
        }

        if (removed) {
            log.trace("{}: Unregistered Reader '{}'.", this.logName, reader.traceObjectId);
        }
    }

    private <T, R> CompletableFuture<R> toCompletableFuture(Future<T> distributedLogFuture, Function<T, R> outputConverter) {
        CompletableFuture<R> resultFuture = new CompletableFuture<>();
        distributedLogFuture.addEventListener(new FutureEventListener<T>() {
            @Override
            public void onSuccess(T outcome) {
                ensureNotClosed();
                resultFuture.complete(outputConverter.apply(outcome));
            }

            @Override
            public void onFailure(Throwable cause) {
                Throwable wrapException = cause;
                if (cause instanceof StreamNotReadyException) {
                    // Temporary (rolling ledgers).
                    wrapException = new DataLogNotAvailableException("DistributedLog Stream not ready.", cause);
                } else if (cause instanceof WriteCancelledException || cause instanceof BKTransmitException) {
                    // General write failure; try again.
                    wrapException = new WriteFailureException("Unable to write data to DistributedLog.", cause);
                } else if (cause instanceof LockingException) {
                    wrapException = new DataLogWriterNotPrimaryException("LogHandle is not exclusive writer for DistributedLog log.", cause);
                } else if (cause instanceof LogRecordTooLongException) {
                    // User error. Record is too long.
                    wrapException = new WriteTooLongException(cause);
                }

                resultFuture.completeExceptionally(wrapException);
            }
        });

        return resultFuture;
    }

    //endregion

    //region DistributedLogReader

    private static class DistributedLogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        final String traceObjectId;
        private final DistributedLogManager logManager;
        private final Consumer<DistributedLogReader> closeCallback;
        private long lastTransactionId;
        private LogReader baseReader;

        //region Constructor

        DistributedLogReader(long afterTransactionId, DistributedLogManager logManager, Consumer<DistributedLogReader> closeCallback) throws IOException {
            Preconditions.checkNotNull(logManager, "logManager");
            Preconditions.checkNotNull(closeCallback, "closeCallback");

            this.traceObjectId = String.format("%s@%d", logManager.getStreamName(), afterTransactionId);
            this.logManager = logManager;
            this.lastTransactionId = afterTransactionId;
            // Add 1 to the transaction id because of different contracts; ours is read after, DL's is read at and after.
            this.baseReader = this.logManager.getInputStream(afterTransactionId + 1);
            this.closeCallback = closeCallback;
        }

        //endregion

        //region CloseableIterator Implementation

        @Override
        public synchronized DurableDataLog.ReadItem getNext() throws DurableDataLogException {
            final long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "getNext");
            try {
                LogRecordWithDLSN baseRecord = this.baseReader.readNext(false); // NonBlocking == false -> Blocking read
                if (baseRecord == null) {
                    log.debug("{}: LogReader.readNext (EndOfStream).", this.traceObjectId);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "getNext", traceId);
                    return null;
                }
                this.lastTransactionId = baseRecord.getTransactionId();
                log.debug("{}: LogReader.readNext (TransactionId {}, Length = {}).", this.traceObjectId, this.lastTransactionId, baseRecord.getPayload().length);
                LoggerHelpers.traceLeave(log, this.traceObjectId, "getNext", traceId);
                return new ReadItem(baseRecord);
            } catch (IOException ex) {
                // TODO: need to hook up a retry policy here.
                throw new DurableDataLogException("Unable to read next item.", ex);
            }
        }

        @Override
        public void close() {
            try {
                // Close base reader.
                this.baseReader.close();

                // Invoke the close callback.
                CallbackHelpers.invokeSafely(this.closeCallback, this, null);
            } catch (IOException ex) {
                log.error("{}: Unable to close LogReader. {}", this.traceObjectId, ex);
            }
        }

        //endregion

        //region ReadItem

        private static class ReadItem implements DurableDataLog.ReadItem {
            private final LogRecordWithDLSN baseRecord;

            ReadItem(LogRecordWithDLSN baseRecord) {
                this.baseRecord = baseRecord;
            }

            @Override
            public byte[] getPayload() {
                return this.baseRecord.getPayload();
            }

            @Override
            public LogAddress getAddress() {
                return new DLSNAddress(this.baseRecord.getTransactionId(), this.baseRecord.getDlsn());
            }

            @Override
            public String toString() {
                return String.format("%s, Length = %d.", getAddress(), getPayload().length);
            }
        }

        //endregion
    }

    //endregion

    //region Metrics

    private static class Metrics {
        private static final StatsLogger DURABLE_DATALOG_LOGGER = MetricsProvider.createStatsLogger("DURABLEDATALOG");
        private static final OpStatsLogger WRITE_LATENCY = DURABLE_DATALOG_LOGGER.createStats(SegmentStoreMetricsNames
                .DURABLE_DATALOG_WRITE_LATENCY);
        private static final Counter WRITE_BYTES = DURABLE_DATALOG_LOGGER.createCounter(SegmentStoreMetricsNames
                .DURABLE_DATALOG_WRITE_BYTES);
    }

    //endregion
}
