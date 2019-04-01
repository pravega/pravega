/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Data
/**
 * This class is the metadata to capture the currently processing transaction commit work. This captures the list of
 * transactions that current round of processing will attempt to commit. If the processing fails and retries, it will
 * find the list of transcations and reattempt to process them in exact same order.
 * This also includes optional "active epoch" field which is set if the commits have to be rolled over because they are
 * over an older epoch.
 */
public class CommittingTransactionsRecord {
    public static final CommitTransactionsRecordSerializer SERIALIZER = new CommitTransactionsRecordSerializer();
    public static final CommittingTransactionsRecord EMPTY = CommittingTransactionsRecord.builder().epoch(Integer.MIN_VALUE).transactionsToCommit(ImmutableList.of()).activeEpoch(Optional.empty()).build();
    /**
     * Epoch from which transactions are committed.
     */
    private final int epoch;
    /**
     * Transactions to be be committed.
     */
    private final List<UUID> transactionsToCommit;

    /**
     * Set only for rolling transactions and identify the active epoch that is being rolled over.
     */
    @Getter(AccessLevel.PRIVATE)
    private Optional<Integer> activeEpoch;

    public CommittingTransactionsRecord(int epoch, List<UUID> transactionsToCommit) {
        this(epoch, transactionsToCommit, Optional.empty(), true);
    }

    public CommittingTransactionsRecord(int epoch, List<UUID> transactionsToCommit, int activeEpoch) {
        this(epoch, transactionsToCommit, Optional.of(activeEpoch), true);
    }
    
    @Builder
    /**
     * This is a private constructor that is only directly used by the builder during the deserialization. 
     * The deserialization passes @param copyCollections as false so that we do not make an immutable copy of the collection
     * for the collection passed to the constructor via deserialization. 
     * 
     * The all other constructors, the value of copyCollections flag is true and we make an immutable collection copy of 
     * the supplied collection. 
     * All getters of this class that return a collection always wrap them under Collections.unmodifiableCollection so that
     * no one can change the data object from outside.  
     */
    private CommittingTransactionsRecord(int epoch, List<UUID> transactionsToCommit, Optional<Integer> activeEpoch, boolean copyCollections) {
        this.epoch = epoch;
        this.transactionsToCommit = copyCollections ? ImmutableList.copyOf(transactionsToCommit) : transactionsToCommit;
        this.activeEpoch = activeEpoch;
    }

    private static class CommittingTransactionsRecordBuilder implements ObjectBuilder<CommittingTransactionsRecord> {
        private Optional<Integer> activeEpoch = Optional.empty();
    }

    public List<UUID> getTransactionsToCommit() {
        return Collections.unmodifiableList(transactionsToCommit);
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
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, CommittingTransactionsRecordBuilder builder)
                throws IOException {
            builder.epoch(revisionDataInput.readInt())
                   .transactionsToCommit(revisionDataInput.readCollection(RevisionDataInput::readUUID, ArrayList::new));

            int read = revisionDataInput.readInt();
            if (read == Integer.MIN_VALUE) {
                builder.activeEpoch(Optional.empty());
            } else {
                builder.activeEpoch(Optional.of(read));
            }
            builder.copyCollections(false);
        }

        private void write00(CommittingTransactionsRecord record, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeInt(record.getEpoch());
            revisionDataOutput.writeCollection(record.getTransactionsToCommit(), RevisionDataOutput::writeUUID);
            revisionDataOutput.writeInt(record.getActiveEpoch().orElse(Integer.MIN_VALUE));
        }

        @Override
        protected CommittingTransactionsRecordBuilder newBuilder() {
            return CommittingTransactionsRecord.builder();
        }
    }
}