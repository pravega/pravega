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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * A serializable class that encapsulates a Stream's State.
 */
@Data
@Builder
@Slf4j
@AllArgsConstructor
public class CommittingTxnsCountRecord {
    public static final CommittingTxnsCountRecordSerializer SERIALIZER = new CommittingTxnsCountRecordSerializer();

    private final Integer committingTransactionsCount;

    public static class CommittingTxnsCountRecordBuilder implements ObjectBuilder<CommittingTxnsCountRecord> {

    }

    @SneakyThrows(IOException.class)
    public static CommittingTxnsCountRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    private static class CommittingTxnsCountRecordSerializer extends VersionedSerializer.WithBuilder<CommittingTxnsCountRecord, CommittingTxnsCountRecord.CommittingTxnsCountRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, CommittingTxnsCountRecord.CommittingTxnsCountRecordBuilder builder) throws IOException {
            int count = revisionDataInput.readCompactInt();
            builder.committingTransactionsCount(count);

        }

        private void write00(CommittingTxnsCountRecord countRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCompactInt(countRecord.getCommittingTransactionsCount());
        }

        @Override
        protected CommittingTxnsCountRecord.CommittingTxnsCountRecordBuilder newBuilder() {
            return CommittingTxnsCountRecord.builder();
        }
    }
}
