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
package io.pravega.controller.store.stream.records;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * This class is the metadata to capture the currently processing transaction commit work. This captures the list of
 * transactions that current round of processing will attempt to commit. If the processing fails and retries, it will
 * find the list of transcations and reattempt to process them in exact same order.
 * This also includes optional "active epoch" field which is set if the commits have to be rolled over because they are
 * over an older epoch.
 */
@Data
public class CommittingTransactionsRecord {
    public static final CommitTransactionsRecordSerializer SERIALIZER = new CommitTransactionsRecordSerializer();
    public static final CommittingTransactionsRecord INITIAL = CommittingTransactionsRecord.builder().epoch(Integer.MIN_VALUE)
                                                                    .transactionsToCommit(ImmutableList.of()).activeEpoch(Optional.empty())
                                                                    .batchId(0L).build();
    /**
     * Epoch from which transactions are committed.
     */
    private final int epoch;
    /**
     * Transactions to be be committed.
     */
    private final ImmutableList<UUID> transactionsToCommit;

    /**
     * Set only for rolling transactions and identify the active epoch that is being rolled over.
     */
    @Getter(AccessLevel.PRIVATE)
    private Optional<Integer> activeEpoch;

    /**
     * Batch Identifier for the last successfully committed transaction batch.
     */
    private long batchId;

    public CommittingTransactionsRecord(int epoch, @NonNull ImmutableList<UUID> transactionsToCommit, long batchId) {
        this(epoch, transactionsToCommit, Optional.empty(), batchId);
    }

    public CommittingTransactionsRecord(int epoch, @NonNull ImmutableList<UUID> transactionsToCommit, int activeEpoch, long batchId) {
        this(epoch, transactionsToCommit, Optional.of(activeEpoch), batchId);
    }

    public static CommittingTransactionsRecord newEmptyCommittingTransactionsRecord(long batchId) {
        return new CommittingTransactionsRecord(Integer.MIN_VALUE, ImmutableList.of(), Optional.empty(), batchId);
    }

    public static long newBatchId(long currentBatchId) {
        return currentBatchId < Long.MAX_VALUE ?  currentBatchId + 1 : 0L;
    }

    public boolean isEmpty() {
        return (this.epoch == INITIAL.epoch &&
                this.transactionsToCommit.equals(INITIAL.transactionsToCommit) &&
                this.activeEpoch.equals(INITIAL.activeEpoch));
    }

    @Builder
    private CommittingTransactionsRecord(int epoch, @NonNull ImmutableList<UUID> transactionsToCommit, Optional<Integer> activeEpoch, long batchId) {
        this.epoch = epoch;
        this.transactionsToCommit = transactionsToCommit;
        this.activeEpoch = activeEpoch;
        this.batchId = batchId;
    }

    private static class CommittingTransactionsRecordBuilder implements ObjectBuilder<CommittingTransactionsRecord> {
        private Optional<Integer> activeEpoch = Optional.empty();
    }

    @SneakyThrows(IOException.class)
    public static CommittingTransactionsRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    public CommittingTransactionsRecord createRollingTxnRecord(int activeEpoch) {
        Preconditions.checkState(!this.activeEpoch.isPresent());
        return new CommittingTransactionsRecord(this.epoch, this.transactionsToCommit, activeEpoch);
    }

    public boolean isRollingTxnRecord() {
        return activeEpoch.isPresent();
    }

    public int getCurrentEpoch() {
        Preconditions.checkState(activeEpoch.isPresent());
        return activeEpoch.get();
    }

    public int getNewTxnEpoch() {
        Preconditions.checkState(activeEpoch.isPresent());
        return activeEpoch.get() + 1;
    }

    public int getNewActiveEpoch() {
        Preconditions.checkState(activeEpoch.isPresent());
        return activeEpoch.get() + 2;
    }

    private static class CommitTransactionsRecordSerializer
            extends VersionedSerializer.WithBuilder<CommittingTransactionsRecord, CommittingTransactionsRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00)
                    .revision(1, this::write01, this::read01);
        }

        private void read00(RevisionDataInput revisionDataInput, CommittingTransactionsRecordBuilder builder)
                throws IOException {
            ImmutableList.Builder<UUID> listBuilder = ImmutableList.builder();
            builder.epoch(revisionDataInput.readInt());

            revisionDataInput.readCollection(RevisionDataInput::readUUID, listBuilder);
            builder.transactionsToCommit(listBuilder.build());

            int read = revisionDataInput.readInt();
            if (read == Integer.MIN_VALUE) {
                builder.activeEpoch(Optional.empty());
            } else {
                builder.activeEpoch(Optional.of(read));
            }
        }

        private void write00(CommittingTransactionsRecord record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(record.getEpoch());
            revisionDataOutput.writeCollection(record.getTransactionsToCommit(), RevisionDataOutput::writeUUID);
            revisionDataOutput.writeInt(record.getActiveEpoch().orElse(Integer.MIN_VALUE));
        }

        private void read01(RevisionDataInput revisionDataInput, CommittingTransactionsRecordBuilder builder)
                throws IOException {
            builder.batchId(revisionDataInput.readLong());
        }

        private void write01(CommittingTransactionsRecord record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(record.getBatchId());
        }

        @Override
        protected CommittingTransactionsRecordBuilder newBuilder() {
            return CommittingTransactionsRecord.builder();
        }
    }
}
