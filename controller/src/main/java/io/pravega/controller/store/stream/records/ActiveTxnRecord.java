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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.store.stream.TxnStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
@Builder
@AllArgsConstructor
@Slf4j
public class ActiveTxnRecord {
    public static final ActiveTxnRecord EMPTY = ActiveTxnRecord.builder().txCreationTimestamp(Long.MIN_VALUE)
            .leaseExpiryTime(Long.MIN_VALUE).maxExecutionExpiryTime(Long.MIN_VALUE).txnStatus(TxnStatus.UNKNOWN)
            .writerId(Optional.empty()).commitTime(Optional.empty()).commitOrder(Optional.empty()).commitOffsets(ImmutableMap.of()).build();
    
    public static final ActiveTxnRecordSerializer SERIALIZER = new ActiveTxnRecordSerializer();

    private final long txCreationTimestamp;
    private final long leaseExpiryTime;
    private final long maxExecutionExpiryTime;
    private final TxnStatus txnStatus;
    private final Optional<String> writerId;
    private final Optional<Long> commitTime;
    private final Optional<Long> commitOrder;
    private final ImmutableMap<Long, Long> commitOffsets;

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus) {
        this.txCreationTimestamp = txCreationTimestamp;
        this.leaseExpiryTime = leaseExpiryTime;
        this.maxExecutionExpiryTime = maxExecutionExpiryTime;
        this.txnStatus = txnStatus;
        this.writerId = Optional.empty();
        this.commitTime = Optional.empty();
        this.commitOrder = Optional.empty();
        this.commitOffsets = ImmutableMap.of();
    }

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus, 
                           String writerId, long commitTime, long commitOrder) {
        this(txCreationTimestamp, leaseExpiryTime, maxExecutionExpiryTime, txnStatus, writerId, commitTime, commitOrder, 
                ImmutableMap.of());
    }

    public ActiveTxnRecord(long txCreationTimestamp, long leaseExpiryTime, long maxExecutionExpiryTime, TxnStatus txnStatus, 
                           String writerId, long commitTime, long commitOrder, ImmutableMap<Long, Long> commitOffsets) {
        this.txCreationTimestamp = txCreationTimestamp;
        this.leaseExpiryTime = leaseExpiryTime;
        this.maxExecutionExpiryTime = maxExecutionExpiryTime;
        this.txnStatus = txnStatus;
        this.writerId = Optional.ofNullable(writerId);
        this.commitTime = Optional.of(commitTime);
        this.commitOrder = Optional.of(commitOrder);
        this.commitOffsets = commitOffsets;
    }

    public String getWriterId() {
        return writerId.orElse("");
    }

    public long getCommitTime() {
        return commitTime.orElse(Long.MIN_VALUE);
    }
    
    public long getCommitOrder() {
        return commitOrder.orElse(Long.MIN_VALUE);
    }

    public static class ActiveTxnRecordBuilder implements ObjectBuilder<ActiveTxnRecord> {
        private Optional<String> writerId = Optional.empty();
        private Optional<Long> commitTime = Optional.empty();
        private ImmutableMap<Long, Long> commitOffsets = ImmutableMap.of();

    }

    @SneakyThrows(IOException.class)
    public static ActiveTxnRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        String record = String.format("%s = %s", "txCreationTimestamp", txCreationTimestamp) + "\n" +
                String.format("%s = %s", "leaseExpiryTime", leaseExpiryTime) + "\n" +
                String.format("%s = %s", "maxExecutionExpiryTime", maxExecutionExpiryTime) + "\n" +
                String.format("%s = %s", "txnStatus", txnStatus) + "\n";
        if (writerId.isPresent()) {
            record = record + String.format("%s = %s", "writerId", writerId.get()) + "\n";
        }
        if (commitTime.isPresent()) {
            record = record + String.format("%s = %s", "commitTime", commitTime.get()) + "\n";
        }
        if (commitOrder.isPresent()) {
            record = record + String.format("%s = %s", "commitOrder", commitOrder.get()) + "\n";
        }
        record = record + String.format("%s = %s", "commitOffsets", commitOffsets.keySet().stream()
                .map(key -> key + " : " + commitOffsets.get(key))
                .collect(Collectors.joining(", ", "{", "}")));
        return record;
    }

    private static class ActiveTxnRecordSerializer
            extends VersionedSerializer.WithBuilder<ActiveTxnRecord, ActiveTxnRecord.ActiveTxnRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00).revision(1, this::write01, this::read01)
                      .revision(2, this::write02, this::read02);
        }

        private void read00(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {
            activeTxnRecordBuilder.txCreationTimestamp(revisionDataInput.readLong())
                                  .leaseExpiryTime(revisionDataInput.readLong())
                                  .maxExecutionExpiryTime(revisionDataInput.readLong())
                                  .txnStatus(TxnStatus.values()[revisionDataInput.readCompactInt()]);
        }

        private void write00(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(activeTxnRecord.getTxCreationTimestamp());
            revisionDataOutput.writeLong(activeTxnRecord.getLeaseExpiryTime());
            revisionDataOutput.writeLong(activeTxnRecord.getMaxExecutionExpiryTime());
            revisionDataOutput.writeCompactInt(activeTxnRecord.getTxnStatus().ordinal());
        }

        private void read01(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {
            activeTxnRecordBuilder.writerId(Optional.of(revisionDataInput.readUTF()))
                                  .commitTime(Optional.of(revisionDataInput.readLong()))
                                  .commitOrder(Optional.of(revisionDataInput.readLong()));
        }

        private void write01(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(activeTxnRecord.getWriterId());
            revisionDataOutput.writeLong(activeTxnRecord.getCommitTime());
            revisionDataOutput.writeLong(activeTxnRecord.getCommitOrder());
        }
        
        private void read02(RevisionDataInput revisionDataInput, ActiveTxnRecord.ActiveTxnRecordBuilder activeTxnRecordBuilder)
                throws IOException {

            ImmutableMap.Builder<Long, Long> mapBuilder = new ImmutableMap.Builder<>();
            revisionDataInput.readMap(DataInput::readLong, DataInput::readLong, mapBuilder);
            activeTxnRecordBuilder.commitOffsets(mapBuilder.build());
        }

        private void write02(ActiveTxnRecord activeTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeMap(activeTxnRecord.commitOffsets, DataOutput::writeLong, DataOutput::writeLong);
        }

        @Override
        protected ActiveTxnRecord.ActiveTxnRecordBuilder newBuilder() {
            return ActiveTxnRecord.builder();
        }
    }
}
