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
import io.pravega.controller.store.stream.TxnStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Data
@Builder
@AllArgsConstructor
@Slf4j
public class CompletedTxnRecord {
    public static final CompletedTxnRecordSerializer SERIALIZER = new CompletedTxnRecordSerializer();

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static class CompletedTxnRecordBuilder implements ObjectBuilder<CompletedTxnRecord> {

    }

    @SneakyThrows(IOException.class)
    public static CompletedTxnRecord fromBytes(final byte[] data) {
        return SERIALIZER.deserialize(data);
    }

    @SneakyThrows(IOException.class)
    public byte[] toBytes() {
        return SERIALIZER.serialize(this).getCopy();
    }

    @Override
    public String toString() {
        return String.format("%s = %s", "completeTime", completeTime) + "\n" +
                String.format("%s = %s", "completionStatus", completionStatus);
    }
    
    private static class CompletedTxnRecordSerializer
            extends VersionedSerializer.WithBuilder<CompletedTxnRecord, CompletedTxnRecord.CompletedTxnRecordBuilder> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput,
                            CompletedTxnRecord.CompletedTxnRecordBuilder completedTxnRecordBuilder)
                throws IOException {
            completedTxnRecordBuilder.completeTime(revisionDataInput.readLong())
                                     .completionStatus(TxnStatus.values()[revisionDataInput.readCompactInt()]);
        }

        private void write00(CompletedTxnRecord completedTxnRecord, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeLong(completedTxnRecord.getCompleteTime());
            revisionDataOutput.writeCompactInt(completedTxnRecord.getCompletionStatus().ordinal());
        }

        @Override
        protected CompletedTxnRecord.CompletedTxnRecordBuilder newBuilder() {
            return CompletedTxnRecord.builder();
        }
    }

}
