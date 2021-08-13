/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Transaction-based Metadata Updater for Log Operations.
 */
@Slf4j
@NotThreadSafe
class OperationMetadataUpdater implements ContainerMetadata {
    //region Members

    private static final long MAX_TRANSACTION = Long.MAX_VALUE;
    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private final ArrayDeque<ContainerMetadataUpdateTransaction> transactions;
    private long nextTransactionId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationMetadataUpdater class.
     *
     * @param metadata The Container Metadata to update.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationMetadataUpdater(UpdateableContainerMetadata metadata) {
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.traceObjectId = String.format("OperationMetadataUpdater[%d]", metadata.getContainerId());
        this.nextTransactionId = 0;
        this.transactions = new ArrayDeque<>();
    }

    //endregion

    //region ContainerMetadata Implementation

    @Override
    public SegmentMetadata getStreamSegmentMetadata(long segmentId) {
        return fromMetadata(m -> m.getStreamSegmentMetadata(segmentId));
    }

    @Override
    public Collection<Long> getAllStreamSegmentIds() {
        return fromMetadata(ContainerMetadata::getAllStreamSegmentIds);
    }

    @Override
    public int getMaximumActiveSegmentCount() {
        return this.metadata.getMaximumActiveSegmentCount(); // This never changes.
    }

    @Override
    public int getActiveSegmentCount() {
        return fromMetadata(ContainerMetadata::getActiveSegmentCount);
    }

    @Override
    public long getStreamSegmentId(String segmentName, boolean updateLastUsed) {
        return fromMetadata(m -> m.getStreamSegmentId(segmentName, updateLastUsed));
    }

    @Override
    public int getContainerId() {
        return this.metadata.getContainerId(); // This never changes.
    }

    @Override
    public long getContainerEpoch() {
        return this.metadata.getContainerEpoch(); // This never changes.
    }

    @Override
    public boolean isRecoveryMode() {
        return this.metadata.isRecoveryMode(); // This never changes.
    }

    @Override
    public long getOperationSequenceNumber() {
        return fromMetadata(ContainerMetadata::getOperationSequenceNumber);
    }

    //endregion

    //region Processing

    /**
     * Seals the current UpdateTransaction (if any) and returns its id.
     *
     * @return The sealed UpdateTransaction Id.
     */
    long sealTransaction() {
        // Even if we have had no changes, still create a new (empty) transaction and seal it, since callers will expect
        // a different Id every time which can be committed/rolled back.
        getOrCreateTransaction().seal();

        // Always return nextTransactionId - 1, since otherwise we are at risk of returning a value we previously returned
        // (for example, if we rolled back a transaction).
        return this.nextTransactionId - 1;
    }

    /**
     * Commits all outstanding changes to the base Container Metadata.
     */
    void commitAll() {
        commit(MAX_TRANSACTION);
    }

    /**
     * Commits all outstanding changes to the base Container Metadata, up to and including the one for the given
     * UpdateTransaction.
     * @param upToTransactionId  The Id of the UpdateTransaction up to which to commit.
     * @return The number of MetadataUpdateTransactions committed.
     */
    int commit(long upToTransactionId) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "commit", upToTransactionId);

        // Commit every UpdateTransaction, in order, until we reach our transaction id.
        List<Long> commits = new ArrayList<>();
        while (!this.transactions.isEmpty() && this.transactions.peekFirst().getTransactionId() <= upToTransactionId) {
            ContainerMetadataUpdateTransaction txn = this.transactions.removeFirst();
            txn.seal();
            txn.commit(this.metadata);
            commits.add(txn.getTransactionId());
        }

        // Rebase the first remaining UpdateTransaction over to the current metadata (it was previously pointing to the
        // last committed UpdateTransaction).
        if (commits.size() > 0 && !this.transactions.isEmpty()) {
            this.transactions.peekFirst().rebase(this.metadata);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "commit", traceId, commits);
        return commits.size();
    }

    /**
     * Discards any outstanding changes, starting at the given UpdateTransaction forward.
     *
     * @param fromTransactionId The Id of the UpdateTransaction from which to rollback.
     */
    void rollback(long fromTransactionId) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollback", fromTransactionId);
        List<Long> rolledBack = new ArrayList<>();
        while (!this.transactions.isEmpty() && this.transactions.peekLast().getTransactionId() >= fromTransactionId) {
            ContainerMetadataUpdateTransaction txn = this.transactions.removeLast();
            txn.seal();
            rolledBack.add(txn.getTransactionId());
        }

        // At this point, the transaction list is either empty or its last one is sealed; any further changes would
        // require creating a new transaction.
        LoggerHelpers.traceLeave(log, this.traceObjectId, "rollback", traceId, rolledBack);
    }

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     */
    long nextOperationSequenceNumber() {
        Preconditions.checkState(!isRecoveryMode(), "Cannot request new Operation Sequence Number in Recovery Mode.");
        return this.metadata.nextOperationSequenceNumber();
    }

    /**
     * Sets the operation sequence number in the current UpdateTransaction.
     */
    void setOperationSequenceNumber(long value) {
        Preconditions.checkState(isRecoveryMode(), "Can only set new Operation Sequence Number in Recovery Mode.");
        getOrCreateTransaction().setOperationSequenceNumber(value);
    }

    /**
     * Phase 1/2 of processing a Operation.
     *
     * If the given operation is a StorageOperation, the Operation is validated against the base Container Metadata and
     * the pending UpdateTransaction and it is updated accordingly (if needed).
     *
     * If the given operation is a MetadataCheckpointOperation, the current state of the metadata (including pending
     * UpdateTransactions) is serialized to it.
     *
     * For all other kinds of MetadataOperations (i.e., StreamSegmentMapOperation) this method only
     * does anything if the base Container Metadata is in Recovery Mode (in which case the given MetadataOperation) is
     * recorded in the pending UpdateTransaction.
     *
     * @param operation The operation to pre-process.
     * @throws ContainerException              If the given operation was rejected given the current state of the container
     * metadata or most recent UpdateTransaction.
     * @throws StreamSegmentNotExistsException If the given operation was for a Segment that was is marked as deleted.
     * @throws StreamSegmentSealedException    If the given operation was for a Segment that was previously sealed and
     *                                         that is incompatible with a sealed Segment.
     * @throws StreamSegmentMergedException    If the given operation was for a Segment that was previously merged.
     * @throws NullPointerException            If the operation is null.
     */
    void preProcessOperation(Operation operation) throws ContainerException, StreamSegmentException {
        log.trace("{}: PreProcess {}.", this.traceObjectId, operation);
        getOrCreateTransaction().preProcessOperation(operation);
    }

    /**
     * Phase 2/2 of processing an Operation. The Operation's effects are reflected in the pending UpdateTransaction.
     *
     * This method only has an effect on StorageOperations. It does nothing for MetadataOperations, regardless of whether
     * the base Container Metadata is in Recovery Mode or not.
     *
     * @param operation The operation to accept.
     * @throws MetadataUpdateException If the given operation was rejected given the current state of the metadata or
     * most recent UpdateTransaction.
     * @throws NullPointerException    If the operation is null.
     */
    void acceptOperation(Operation operation) throws MetadataUpdateException {
        log.trace("{}: Accept {}.", this.traceObjectId, operation);
        getOrCreateTransaction().acceptOperation(operation);
    }

    /**
     * Returns the result of the given function applied either to the current UpdateTransaction (if any), or the base metadata,
     * if no UpdateTransaction exists.
     *
     * @param getter The Function to apply.
     * @param <T>    Result type.
     * @return The result of the given Function.
     */
    private <T> T fromMetadata(Function<ContainerMetadata, T> getter) {
        ContainerMetadataUpdateTransaction txn = getActiveTransaction();
        return getter.apply(txn == null ? this.metadata : txn);
    }

    private ContainerMetadataUpdateTransaction getActiveTransaction() {
        if (this.transactions.isEmpty()) {
            return null;
        }

        ContainerMetadataUpdateTransaction last = this.transactions.peekLast();
        if (last.isSealed()) {
            return null;
        } else {
            return last;
        }
    }

    private ContainerMetadataUpdateTransaction getOrCreateTransaction() {
        if (this.transactions.isEmpty() || this.transactions.peekLast().isSealed()) {
            // No transactions or last transaction is sealed. Create a new one.
            ContainerMetadata previous = this.metadata;
            if (!this.transactions.isEmpty()) {
                previous = this.transactions.peekLast();
            }

            ContainerMetadataUpdateTransaction txn = new ContainerMetadataUpdateTransaction(previous, this.metadata, this.nextTransactionId);
            this.nextTransactionId++;
            this.transactions.addLast(txn);
        }

        return this.transactions.peekLast();
    }

    //endregion
}
