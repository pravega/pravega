package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.*;
import com.twitter.distributedlog.*;
import com.twitter.distributedlog.exceptions.LogEmptyException;
import com.twitter.distributedlog.exceptions.OwnershipAcquireFailedException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

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
class LogHandle implements AutoCloseable {
    private final DistributedLogManager logManager;
    private final AsyncLogWriter logWriter;
    private final AtomicLong lastTransactionId;
    private final String logName;
    private final HashSet<DistributedLogReader> activeReaders;
    private boolean closed;

    /**
     * @param namespace
     * @param logName
     * @throws DataLogInitializationException
     * @throws DataLogWriterNotPrimaryException
     * @throws DataLogNotAvailableException
     * @throws DurableDataLogException
     */
    public LogHandle(DistributedLogNamespace namespace, String logName) throws DurableDataLogException {
        this.logName = logName;
        this.lastTransactionId = new AtomicLong(0);
        this.activeReaders = new HashSet<>();

        // Initialize Log Manager, Log Writer and Log Reader.
        boolean success = false;
        try {
            this.logManager = namespace.openLog(logName);
            this.logWriter = this.logManager.startAsyncLogSegmentNonPartitioned();
            //this.logManager.getInputStream(0)
            success = true;
        }
        catch (OwnershipAcquireFailedException ex) {
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
                    System.err.println(ex); // TODO: fix this.
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
            System.err.println(ex); // TODO: fix this.
        }
    }

    public long getLastTransactionId() {
        return this.lastTransactionId.get();
    }

    public CompletableFuture<Long> append(InputStream data, java.time.Duration timeout) {
        ensureNotClosed();

        final long transactionId = this.lastTransactionId.incrementAndGet();
        CompletableFuture<Long> resultFuture = new CompletableFuture<>();

        // This extra buffer allocation + copy is necessary because DL Writer only accepts entire byte[] arrays
        // (it doesn't provide an API for InputStream or byte[] + offset + length.
        byte[] buffer;
        try {
            buffer = new byte[data.available()];
            int bytesRead = StreamHelpers.readAll(data, buffer, 0, buffer.length);
            if (bytesRead != buffer.length) {
                throw new AssertionError(String.format("StreamHelpers.ReadAll did not getReader entire input stream. Expected %d, Actual %d.", buffer.length, bytesRead));
            }
        }
        catch (IOException ex) {
            resultFuture.completeExceptionally(ex);
            return resultFuture;
        }

        // Send the write to DistributedLog.
        Future<DLSN> writeFuture = this.logWriter.write(new LogRecord(transactionId, buffer));
        writeFuture.addEventListener(new FutureEventListener<DLSN>() {
            @Override
            public void onSuccess(DLSN value) {
                ensureNotClosed();
                resultFuture.complete(transactionId);
            }

            @Override
            public void onFailure(Throwable cause) {
                resultFuture.completeExceptionally(cause);
            }
        });

        return resultFuture;
    }

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

        return reader;
    }

    public CompletableFuture<Void> truncate(long upToTransactionId, java.time.Duration timeout) {
        ensureNotClosed();
        //TODO: implement.
        return null;
    }

    @Override
    public void close() throws DurableDataLogException {
        if (!this.closed) {
            try {
                // Check for null for each of these components since we may be attempting to close them from the constructor
                // due to an initialization failure.
                if (this.logWriter != null) {
                    FutureUtils.result(this.logWriter.asyncClose());
                }

                if (this.logManager != null) {
                    FutureUtils.result(this.logManager.asyncClose());
                }
            }
            catch (IOException ex) {
                throw new DurableDataLogException(String.format("Unable to close LogHandle for log '%s'.", this.logName), ex);
            }

            this.closed = true;

            // Close all active readers.
            List<DistributedLogReader> readersToClose;
            synchronized (this.activeReaders) {
                readersToClose = new ArrayList<>(this.activeReaders);
            }

            readersToClose.forEach(DistributedLogReader::close);
            assert this.activeReaders.size() == 0;
        }
    }

    @Override
    public String toString() {
        return this.logName;
    }

    private void ensureNotClosed() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }
    }

    private void unregisterReader(DistributedLogReader reader) {
        synchronized (this.activeReaders) {
            this.activeReaders.remove(reader);
        }
    }

    //region DistributedLogReader

    private static class DistributedLogReader implements AsyncIterator<DurableDataLog.ReadItem> {
        private final DistributedLogManager logManager;
        private final Consumer<DistributedLogReader> closeCallback;
        private long lastTransactionId;
        private LogReader baseReader;

        //region Constructor

        public DistributedLogReader(long afterTransactionId, DistributedLogManager logManager, Consumer<DistributedLogReader> closeCallback) throws IOException {
            if (logManager == null) {
                throw new NullPointerException("logManager");
            }

            if (closeCallback == null) {
                throw new NullPointerException("closeCallback");
            }

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
                    LogRecordWithDLSN baseRecord = this.baseReader.readNext(false); // NonBlocking == false -> Blocking getReader
                    if (baseRecord == null) {
                        return null;
                    }
                    this.lastTransactionId = baseRecord.getTransactionId();
                    return new ReadItem(baseRecord);
                }
                catch (IOException ex) {
                    // TODO: need to hook up a retry policy here.
                    throw new CompletionException(new DurableDataLogException("Unable to getReader next item.", ex));
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
                System.err.println(ex); //TODO: fix
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
