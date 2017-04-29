/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;

import io.pravega.ClientFactory;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.Serializer;
import io.pravega.stream.Transaction;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Flink sink implementation for writing into pravega storage.
 *
 * @param <T> The type of the event to be written.
 */
@Slf4j
public class FlinkExactlyOncePravegaWriter<T> 
        extends RichSinkFunction<T>
        implements ListCheckpointed<UUID>, CheckpointListener {

    private static final long serialVersionUID = 1L;

    // the numbers below are picked somewhat arbitrarily at this point

    private static final long DEFAULT_TXN_TIMEOUT_MILLIS = 2 * 60 * 60 * 1000; // 2 hours

    private static final long DEFAULT_TX_SCALE_GRCASE_MILLIS = 10 * 60 * 1000; // 10 minutes

    // ----------- configuration fields -----------

    // The supplied event serializer.
    private final SerializationSchema<T> serializationSchema;

    // The router used to partition events within a stream.
    private final PravegaEventRouter<T> eventRouter;

    // The pravega controller endpoint.
    private final URI controllerURI;

    // The scope name of the destination stream.
    private final String scopeName;

    // The pravega stream name to write events to.
    private final String streamName;

    private final long txnTimeoutMillis;
    private final long txnMaxTimeMillis;
    private final long txnGracePeriodMillis;

    // ----------- runtime fields -----------
    
    /** The pravega writer client. Used only to create transactions. */
    private transient EventStreamWriter<T> pravegaWriter;

    /** The currently running transaction to which we write */
    private transient Transaction<T> currentTxn;

    /** The transactions that are complete from Flink's view (their checkpoint was triggered),
     * but not fully committed, because their corresponding checkpoint is not yet confirmed */
    private transient ArrayDeque<TransactionAndCheckpoint<T>> txnsPendingCommit;

    /**
     * The flink pravega writer instance which can be added as a sink to a flink job.
     *
     * @param controllerURI         The pravega controller endpoint address.
     * @param scope                 The destination stream's scope name.
     * @param streamName            The destination stream Name.
     * @param serializationSchema   The implementation for serializing every event into pravega's storage format.
     * @param router                The implementation to extract the partition key from the event.
     */
    public FlinkExactlyOncePravegaWriter(
            final URI controllerURI,
            final String scope,
            final String streamName,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> router) {

        this(controllerURI, scope, streamName, serializationSchema, router,
                DEFAULT_TXN_TIMEOUT_MILLIS, DEFAULT_TXN_TIMEOUT_MILLIS, DEFAULT_TX_SCALE_GRCASE_MILLIS);
    }

    public FlinkExactlyOncePravegaWriter(
            final URI controllerURI,
            final String scope,
            final String streamName,
            final SerializationSchema<T> serializationSchema,
            final PravegaEventRouter<T> router,
            final long txnTimeoutMillis,
            final long txnMaxTimeMillis,
            final long txnGracePeriodMillis) {

        Preconditions.checkNotNull(controllerURI);
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(serializationSchema);
        Preconditions.checkNotNull(router);
        Preconditions.checkArgument(txnTimeoutMillis > 0);
        Preconditions.checkArgument(txnMaxTimeMillis > 0);
        Preconditions.checkArgument(txnGracePeriodMillis > 0);

        this.controllerURI = controllerURI;
        this.scopeName = scope;
        this.streamName = streamName;
        this.serializationSchema = serializationSchema;
        this.eventRouter = router;

        this.txnTimeoutMillis = txnTimeoutMillis;
        this.txnMaxTimeMillis = txnMaxTimeMillis;
        this.txnGracePeriodMillis = txnGracePeriodMillis;
        
        
    }

    // ------------------------------------------------------------------------
    
    @Override
    public void open(Configuration parameters) throws Exception {
        final Serializer<T> serializer = new FlinkSerializer<>(serializationSchema);

        ClientFactory clientFactory = ClientFactory.withScope(this.scopeName, this.controllerURI);
        this.pravegaWriter = clientFactory.createEventWriter(
                this.streamName,
                serializer,
                EventWriterConfig.builder().build());

        log.info("Initialized pravega writer for stream: {}/{} with controller URI: {}", this.scopeName,
                 this.streamName, this.controllerURI);

        // start the transaction that will hold the elements till the first checkpoint
        this.currentTxn = this.pravegaWriter.beginTxn(txnTimeoutMillis, txnMaxTimeMillis, txnGracePeriodMillis);

        log.debug("{} - started first transaction '{}'", name(), this.currentTxn.getTxnId());

        this.txnsPendingCommit = new ArrayDeque<>();
    }

    @Override
    public void invoke(T event) throws Exception {
        this.currentTxn.writeEvent(this.eventRouter.getRoutingKey(event), event);
    }

    @Override
    public void close() throws Exception {
        // close() is the general cleanup method, called on successful and
        // unsuccessful termination

        Exception suppressed = null;

        Transaction<?> txn = this.currentTxn;
        if (txn != null) {
            try {
                txn.abort();
            } catch (Exception e) {
                // we need this to work around the fact that the InterruptedException is sneakily thrown
                //noinspection ConstantConditions
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                suppressed = e;
            }
        }

        EventStreamWriter<T> pravegaWriter = this.pravegaWriter;
        if (pravegaWriter != null) {
            try {
                pravegaWriter.close();
            } catch (Exception e) {
                if (suppressed != null) {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public List<UUID> snapshotState(long checkpointId, long checkpointTime) throws Exception {
        // this is like the pre-commit of a 2-phase-commit transaction
        // we are ready to commit and remember the transaction

        final Transaction<T> txn = this.currentTxn;
        Preconditions.checkState(txn != null, "bug: no transaction object when performing state snapshot");

        if (log.isDebugEnabled()) {
            log.debug("{} - checkpoint {} triggered, flushing transaction '{}'", name(), checkpointId, txn.getTxnId());
        }

        // make sure all events go out
        txn.flush();

        // TODO: should we ping here to extend the lifetime of the transaction?
        //       would that be different to setting a long lifetime from the beginning?

        // remember the transaction to be committed when the checkpoint is confirmed
        this.txnsPendingCommit.addLast(new TransactionAndCheckpoint<>(txn, checkpointId));

        // start the next transaction for what comes after this checkpoint
        this.currentTxn = this.pravegaWriter.beginTxn(txnTimeoutMillis, txnMaxTimeMillis, txnGracePeriodMillis);

        log.debug("{} - started new transaction '{}'", name(), this.currentTxn.getTxnId());
        log.debug("{} - storing pending transactions {}", name(), txnsPendingCommit);

        // store all pending transactions in the checkpoint state
        return txnsPendingCommit.stream().map( (v) -> v.transaction().getTxnId() ).collect(Collectors.toList());
    }

    /**
     * Restores the state, which here means the IDs of transaction for which we have
     * to ensure that they are really committed.
     */
    @Override
    public void restoreState(List<UUID> list) throws Exception {
        // when we are restored with a UUID, we don't really know whether the
        // transaction was already committed, or whether there was a failure between
        // completing the checkpoint on the master, and notifying the writer here.

        // (the common case is actually that is was already committed, the window
        // between the commit on the master and the notification here is very small)

        // it is possible to not have any transactions at all if there was a failure before
        // the first completed checkpoint, or in case of a scale-out event, where some of the
        // new task do not have and transactions assigned to check)
        
        // we can have more than one transaction to check in case of a scale-in event, or
        // for the reasons discussed in the 'notifyCheckpointComplete()' method.
        
        if (list != null && list.size() > 0) {

            final Serializer<?> dummySerializer = new FlinkSerializer<>(null);
            final ClientFactory clientFactory = ClientFactory.withScope(this.scopeName, this.controllerURI);

            final EventStreamWriter<?> pravegaWriter = clientFactory.createEventWriter(
                    this.streamName,
                    dummySerializer,
                    EventWriterConfig.builder().build());

            // go over all transactions that we got. there may be more than one in case of
            // a scale-in and
            for (UUID txnId : list) {
                if (txnId != null) {
                    final Transaction<?> txn = pravegaWriter.getTxn(txnId);
                    final Transaction.Status status = txn.checkStatus();

                    if (status == Transaction.Status.OPEN) {
                        // that is the case when a crash happened between when the master committed
                        // the checkpoint, and the sink could be notified
                        log.info("{} - committing completed checkpoint transaction {} after task restore", 
                                name(), txnId);

                        txn.commit();

                        log.debug("{} - committed checkpoint transaction {}", name(), txnId);

                    } else if (status == Transaction.Status.COMMITTED || status == Transaction.Status.COMMITTING) {
                        // that the common case
                        log.debug("{} - at restore, transaction {} was already committed", name(), txnId);

                    } else {
                        log.warn("{} - found unexpected transaction status {} for transaction {} on task restore. " +
                                "Transaction probably timed out between failure and restore. ", name(), status, txnId);
                    }
                }
            }
        }
    }

    /**
     * Notifies the writer that a checkpoint is complete.
     * 
     * <p>This call happens when the checkpoint has been fully committed
     * (= second part of a two phase commit).
     * 
     * <p>This method is called under a mutually exclusive lock from
     * the invoke() and trigger/restore methods, so there is no
     * need for additional synchronization.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // the following scenarios are possible here
        // 
        //  (1) there is exactly one transaction from the latest checkpoint that
        //      was triggered and completed. That should be the common case.
        //      Simply commit that transaction in that case.
        //
        //  (2) there are multiple pending transactions because one previous
        //      checkpoint was skipped. That is a rare case, but can happen
        //      for example when:
        // 
        //        - the master cannot persist the metadata of the last
        //          checkpoint (temporary outage in the storage system) but
        //          could persist a successive checkpoint (the one notified here)
        //
        //        - other (non Pravega sink) tasks could not persist their status during
        //          the previous checkpoint, but did not trigger a failure because they
        //          could hold onto their state and could successfully persist it in
        //          a successive checkpoint (the one notified here)
        //
        //      In both cases, the prior checkpoint never reach a committed state, but 
        //      this checkpoint is always expected to subsume the prior one and cover all
        //      changes since the last successful one As a consequence, we need to commit
        //      all pending transactions.
        //
        //  (3) Multiple transactions are pending, but the checkpoint complete notification
        //      relates not to the latest. That is possible, because notification messages
        //      can be delayed (in an extreme case till arrive after a succeeding checkpoint
        //      was triggered) and because there can be concurrent overlapping checkpoints
        //      (a new one is started before the previous fully finished).
        //
        // ==> There should never be a case where we have no pending transaction here
        //

        if (txnsPendingCommit.isEmpty()) {
            throw new IllegalStateException("checkpoint completed, but no transaction pending");
        }

        TransactionAndCheckpoint<T> txn;
        while ((txn = txnsPendingCommit.peekFirst()) != null && txn.checkpointId() <= checkpointId) {
            txnsPendingCommit.removeFirst();

            log.info("{} - checkpoint {} complete, committing completed checkpoint transaction {}",
                    name(), checkpointId, txn.transaction().getTxnId());

            // the big assumption is that this now actually works and that the transaction has not timed out, yet

            // TODO: currently, if this fails, there is actually data loss
            //       the task will go into a failure/recovery and on recovery will re-try to commit
            //       which fails again and will eventually be ignored.
            txn.transaction().commit();

            log.debug("{} - committed checkpoint transaction {}", name(), txn.transaction().getTxnId());

        }
    }

    private String name() {
        return getRuntimeContext().getTaskNameWithSubtasks();
    }

    // ------------------------------------------------------------------------
    //  serializer
    // ------------------------------------------------------------------------

    private static final class FlinkSerializer<T> implements Serializer<T> {

        private final SerializationSchema<T> serializationSchema;

        FlinkSerializer(SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
        }

        @Override
        public ByteBuffer serialize(T value) {
            return ByteBuffer.wrap(serializationSchema.serialize(value));
        }

        @Override
        public T deserialize(ByteBuffer serializedValue) {
            throw new IllegalStateException("deserialize() called within a serializer");
        }
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    private static final class TransactionAndCheckpoint<T> {

        private final Transaction<T> transaction;
        private final long checkpointId;

        TransactionAndCheckpoint(Transaction<T> transaction, long checkpointId) {
            this.transaction = transaction;
            this.checkpointId = checkpointId;
        }

        Transaction<T> transaction() { return transaction; }

        long checkpointId() { return checkpointId; }

        @Override
        public String toString() {
            return "(checkpoint: " + checkpointId + ", transaction: " + transaction.getTxnId() + ')';
        }
    }
}
