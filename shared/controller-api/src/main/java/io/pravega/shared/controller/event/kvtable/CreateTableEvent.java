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
package io.pravega.shared.controller.event.kvtable;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class CreateTableEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final String scopeName;
    private final String kvtName;
    private final int partitionCount;
    private final int primaryKeyLength;
    private final int secondaryKeyLength;
    private final long timestamp;
    private final long requestId;
    private final UUID tableId;
    private final long rolloverSizeBytes;

    @Override
    public String getKey() {
        return String.format("%s/%s", scopeName, kvtName);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((TableRequestProcessor) processor).processCreateKVTable(this);
    }

    //region Serialization
    private static class CreateTableEventBuilder implements ObjectBuilder<CreateTableEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CreateTableEvent, CreateTableEventBuilder> {
        @Override
        protected CreateTableEventBuilder newBuilder() {
            return CreateTableEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
            version(0).revision(1, this::write01, this::read01);
        }

        private void write00(CreateTableEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scopeName);
            target.writeUTF(e.kvtName);
            target.writeInt(e.partitionCount);
            target.writeLong(e.timestamp);
            target.writeLong(e.requestId);
            target.writeUUID(e.tableId);
            target.writeInt(e.primaryKeyLength);
            target.writeInt(e.secondaryKeyLength);
        }

        private void read00(RevisionDataInput source, CreateTableEventBuilder eb) throws IOException {
            eb.scopeName(source.readUTF());
            eb.kvtName(source.readUTF());
            eb.partitionCount(source.readInt());
            eb.timestamp(source.readLong());
            eb.requestId(source.readLong());
            eb.tableId(source.readUUID());
            eb.primaryKeyLength(source.readInt());
            eb.secondaryKeyLength(source.readInt());
        }

        private void write01(CreateTableEvent e, RevisionDataOutput target) throws IOException {
            target.writeLong(e.rolloverSizeBytes);
        }

        private void read01(RevisionDataInput source, CreateTableEventBuilder eb) throws IOException {
            eb.rolloverSizeBytes(source.readLong());
        }
    }
    //endregion
}
