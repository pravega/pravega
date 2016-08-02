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

import com.emc.nautilus.common.util.CloseableIterator;
import com.emc.logservice.storageabstraction.DataLogInitializationException;
import com.emc.logservice.storageabstraction.DataLogNotAvailableException;
import com.emc.logservice.storageabstraction.DataLogWriterNotPrimaryException;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;
import com.emc.logservice.storageabstraction.WriteFailureException;
import com.emc.logservice.storageabstraction.WriteTooLongException;
import com.emc.nautilus.common.function.CallbackHelpers;
import com.emc.nautilus.common.Exceptions;
import com.emc.nautilus.common.concurrent.FutureHelpers;
import com.emc.nautilus.common.io.StreamHelpers;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Read/Write handle for a particular DistributedLog Stream.
 */
@Slf4j
class LogHandle implements AutoCloseable {
    //region Members
    /**
     * Maximum append length, as specified by DistributedLog (this is hardcoded inside DLog's code).
     */
    public static final int MAX_APPEND_LENGTH = 1024 * 1024 - 8 * 1024 - 20;

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
    public LogHandle(String logName, Consumer<LogHandle> handleClosedCallback) {
        Preconditions.checkNotNull(handleClosedCallback, "handleClosedCallback");
        Exceptions.checkNotNullOrEmpty(logName, "logName");

        this.logName = logName;
        this.handleClosedCallback = handleClosedCallback;
        this.lastTransactionId = new AtomicLong(0);
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
     * @throws DataLogInitializationException
     * @throws DataLogWriterNotPrimaryException
     * @throws DataLogNotAvailableException
     * @throws DurableDataLogException
     */
    public void initialize(DistributedLogNamespace namespace) throws DurableDataLogException {
        Preconditions.checkNotNull(namespace, "namespace");
        Preconditions.checkState(this.logManager == null, "LogHandle is already initialized.");

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
            this.lastTransactionId.set(0);
        } catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to determine last transaction Id for log '%s'.", logName), ex);
        }

        log.info("{}: Initialized (LastTransactionId = {}).", this.logName, this.lastTransactionId);
    }

    /**
     * Gets a value indicating the DistributedLog LogName for this Handle.
     *
     * @return
     */
    public String getLogName() {
        return this.logName;
    }

    /**
     * Gets a value indicating the Id of the Last Transaction that was processed through this Handle/Log. If the Log is
     * currently empty, its value is 0.
     *
     * @return
     */
    public long getLastTransactionId() {
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
    public CompletableFuture<Long> append(InputStream data, java.time.Duration timeout) {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");
        Preconditions.checkNotNull(data, "data");

        final long transactionId = this.lastTransactionId.incrementAndGet();
        CompletableFuture<Long> resultFuture = new CompletableFuture<>();

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
            resultFuture.completeExceptionally(ex);
            return resultFuture;
        }

        // Send the write to DistributedLog.
        log.debug("{}: LogWriter.write (TransactionId = {}, Length = {}).", transactionId, buffer.length);
        Future<DLSN> writeFuture = this.logWriter.write(new LogRecord(transactionId, buffer));
        writeFuture.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN value) {
                ensureNotClosed();
                resultFuture.complete(transactionId);
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

    /**
     * Creates a new Reader starting after the given Transaction Id.
     *
     * @param afterTransactionId The Transaction Id right before where to start.
     * @return A CloseableIterator that returns DurableDataLog.ReadItems, each corresponding to an entry in the DurableLog Read.
     * @throws DurableDataLogException If the reader could not be opened.
     */
    public CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> getReader(long afterTransactionId) throws DurableDataLogException {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");

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

        log.trace("{}: Registered Reader '{}'.", this.logName, reader.traceObjectId);
        return reader;
    }

    public CompletableFuture<Boolean> truncate(long upToTransactionId, java.time.Duration timeout) {
        ensureNotClosed();
        Preconditions.checkState(this.logManager != null, "LogHandle is not initialized.");

        log.info("{}: Truncate (TransactionId = {}.", this.logName, upToTransactionId);
        throw new NotImplementedException("Truncate has not yet been implemented.");
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

    //endregion

    //region DistributedLogReader

    private static class DistributedLogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        public final String traceObjectId;
        private final DistributedLogManager logManager;
        private final Consumer<DistributedLogReader> closeCallback;
        private long lastTransactionId;
        private LogReader baseReader;

        //region Constructor

        public DistributedLogReader(long afterTransactionId, DistributedLogManager logManager, Consumer<DistributedLogReader> closeCallback) throws IOException {
            Preconditions.checkNotNull(logManager, "logManager");
            Preconditions.checkNotNull(closeCallback, "closeCallback");

            this.traceObjectId = String.format("%s@%d", logManager.getStreamName(), afterTransactionId);
            this.logManager = logManager;
            this.lastTransactionId = afterTransactionId;
            this.baseReader = this.logManager.getInputStream(afterTransactionId);
            this.closeCallback = closeCallback;
        }

        //endregion

        //region CloseableIterator Implementation

        @Override
        public synchronized DurableDataLog.ReadItem getNext() throws DurableDataLogException {
            try {
                LogRecordWithDLSN baseRecord = this.baseReader.readNext(false); // NonBlocking == false -> Blocking read
                if (baseRecord == null) {
                    log.debug("{}: LogReader.readNext (EndOfStream).", this.traceObjectId);
                    return null;
                }

                this.lastTransactionId = baseRecord.getTransactionId();
                log.debug("{}: LogReader.readNext (TransactionId {}, Length = {}).", this.traceObjectId, this.lastTransactionId, baseRecord.getPayload().length);
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

            public ReadItem(LogRecordWithDLSN baseRecord) {
                this.baseRecord = baseRecord;
            }

            @Override
            public byte[] getPayload() {
                return this.baseRecord.getPayload();
            }

            @Override
            public long getSequence() {
                return this.baseRecord.getTransactionId();
            }

            @Override
            public String toString() {
                return String.format("Sequence = %d, Length = %d.", this.getSequence(), this.getPayload().length);
            }
        }

        //endregion
    }

    //endregion
}
