package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.*;
import com.twitter.distributedlog.*;
import com.twitter.distributedlog.exceptions.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    public static final int MaxAppendLength = 1024 * 1024 - 8 * 1024;

    private final DistributedLogManager logManager;
    private final AsyncLogWriter logWriter;
    private final AtomicLong lastTransactionId;
    private final String logName;
    private final HashSet<DistributedLogReader> activeReaders;
    private final Consumer<LogHandle> handleClosedCallback;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * @param namespace
     * @param logName
     * @throws DataLogInitializationException
     * @throws DataLogWriterNotPrimaryException
     * @throws DataLogNotAvailableException
     * @throws DurableDataLogException
     */
    public LogHandle(DistributedLogNamespace namespace, String logName, Consumer<LogHandle> handleClosedCallback) throws DurableDataLogException {
        Exceptions.throwIfNull(namespace, "namespace");
        Exceptions.throwIfNull(handleClosedCallback, "handleClosedCallback");
        Exceptions.throwIfNullOfEmpty(logName, "logName");

        this.logName = logName;
        this.handleClosedCallback = handleClosedCallback;
        this.lastTransactionId = new AtomicLong(0);
        this.activeReaders = new HashSet<>();

        // Initialize Log Manager and Log Writer.
        boolean success = false;
        try {
            this.logManager = namespace.openLog(logName);
            log.debug("{}: Opened LogManager.", this.logName);

            this.logWriter = this.logManager.startAsyncLogSegmentNonPartitioned();
            log.debug("{}: Opened LogWriter.", this.logName);
            success = true;
        }
        catch (OwnershipAcquireFailedException ex) {
            // This means one of two things:
            // 1. Someone else currently holds the exclusive write lock.
            // 2. Someone else held the exclusive write lock, crashed, and ZooKeeper did not figure it out yet (due to a long Session Timeout).
            throw new DataLogWriterNotPrimaryException(String.format("Unable to acquire exclusive Writer for log '%s'.", logName), ex);
        }
        catch (IOException ex) {
            // Log does not exist or some other issue happened. Note that LogNotFoundException inherits from IOException, so it's also handled here.
            throw new DataLogNotAvailableException(String.format("Unable to create DistributedLogManager for log '%s'.", logName), ex);
        }
        catch (Exception ex) {
            // General exception, configuration issue, etc.
            throw new DataLogInitializationException(String.format("Unable to create DistributedLogManager for log '%s'.", logName), ex);
        }
        finally {
            if (!success) {
                try {
                    close();
                }
                catch (Exception ex) {
                    log.error("Unable to cleanup resources after the failed attempt to create a LogHandle for '{}'. {}", logName, ex);
                }
            }
        }

        try {
            this.lastTransactionId.set(this.logManager.getLastTxId());
        }
        catch (LogEmptyException ex) {
            this.lastTransactionId.set(0);
        }
        catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to determine last transaction Id for log '%s'.", logName), ex);
        }

        log.info("{}: Initialized (LastTransactionId = {}).", this.logName, this.lastTransactionId);
    }

    //endregion

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
        Exceptions.throwIfNull(data, "data");

        final long transactionId = this.lastTransactionId.incrementAndGet();
        CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        // This extra buffer allocation + copy is necessary because DL Writer only accepts entire byte[] arrays
        // (it doesn't provide an API for InputStream or byte[] + offset + length).
        byte[] buffer;
        try {
            int dataLength = data.available();
            if (dataLength > MaxAppendLength) {
                return FutureHelpers.failedFuture(new WriteTooLongException(dataLength, MaxAppendLength));
            }

            buffer = new byte[dataLength];
            int bytesRead = StreamHelpers.readAll(data, buffer, 0, buffer.length);
            assert bytesRead == buffer.length : String.format("StreamHelpers.ReadAll did not read entire input stream. Expected %d, Actual %d.", buffer.length, bytesRead);
        }
        catch (IOException ex) {
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
                }
                else if (cause instanceof WriteCancelledException || cause instanceof BKTransmitException) {
                    // General write failure; try again.
                    wrapException = new WriteFailureException("Unable to write data to DistributedLog.", cause);
                }
                else if (cause instanceof LockingException) {
                    wrapException = new DataLogWriterNotPrimaryException("LogHandle is not exclusive writer for DistributedLog log.", cause);
                }
                else if (cause instanceof LogRecordTooLongException) {
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
     * @return An AsyncInterator that returns DurableDataLog.ReadItems, each corresponding to an entry in the DurableLog Read.
     * @throws DurableDataLogException If the reader could not be opened.
     */
    public AsyncIterator<DurableDataLog.ReadItem> getReader(long afterTransactionId) throws DurableDataLogException {
        ensureNotClosed();
        DistributedLogReader reader;
        try {
            reader = new DistributedLogReader(afterTransactionId, this.logManager, this::unregisterReader);
        }
        catch (IOException ex) {
            throw new DurableDataLogException("Could not open reader.", ex);
        }

        // Keep track of the newly created reader.
        synchronized (this.activeReaders) {
            this.activeReaders.add(reader);
        }

        log.trace("{}: Registered Reader '{}'.", this.logName, reader.traceObjectId);
        return reader;
    }

    public CompletableFuture<Void> truncate(long upToTransactionId, java.time.Duration timeout) {
        ensureNotClosed();
        log.info("{}: Truncate (TransactionId = {}.", this.logName, upToTransactionId);
        //TODO: implement.
        return null;
    }

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
            }
            catch (IOException ex) {
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

    @Override
    public String toString() {
        return this.logName;
    }

    private void ensureNotClosed() {
        Exceptions.throwIfClosed(this.closed, this);
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

    //region DistributedLogReader

    private static class DistributedLogReader implements AsyncIterator<DurableDataLog.ReadItem> {
        public final String traceObjectId;
        private final DistributedLogManager logManager;
        private final Consumer<DistributedLogReader> closeCallback;
        private long lastTransactionId;
        private LogReader baseReader;

        //region Constructor

        public DistributedLogReader(long afterTransactionId, DistributedLogManager logManager, Consumer<DistributedLogReader> closeCallback) throws IOException {
            Exceptions.throwIfNull(logManager, "logManager");
            Exceptions.throwIfNull(closeCallback, "closeCallback");

            this.traceObjectId = String.format("%s@%d", logManager.getStreamName(), afterTransactionId);
            this.logManager = logManager;
            this.lastTransactionId = afterTransactionId;
            this.baseReader = this.logManager.getInputStream(afterTransactionId);
            this.closeCallback = closeCallback;
        }

        //endregion

        //region AsyncIterator Implementation

        @Override
        public CompletableFuture<DurableDataLog.ReadItem> getNext(Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    LogRecordWithDLSN baseRecord = this.baseReader.readNext(false); // NonBlocking == false -> Blocking read
                    if (baseRecord == null) {
                        log.debug("{}: LogReader.readNext (EndOfStream).", this.traceObjectId);
                        return null;
                    }

                    this.lastTransactionId = baseRecord.getTransactionId();
                    log.debug("{}: LogReader.readNext (TransactionId {}, Length = {}).", this.traceObjectId, this.lastTransactionId, baseRecord.getPayload().length);
                    return new ReadItem(baseRecord);
                }
                catch (IOException ex) {
                    // TODO: need to hook up a retry policy here.
                    throw new CompletionException(new DurableDataLogException("Unable to read next item.", ex));
                }
            });
        }

        @Override
        public void close() {
            try {
                // Close base reader.
                this.baseReader.close();

                // Invoke the close callback.
                CallbackHelpers.invokeSafely(this.closeCallback, this, null);
            }
            catch (IOException ex) {
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
