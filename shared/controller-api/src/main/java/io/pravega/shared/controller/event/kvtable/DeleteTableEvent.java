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
public class DeleteTableEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String kvtName;
    private final long requestId;
    private final UUID tableId;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, kvtName);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((TableRequestProcessor) processor).processDeleteKVTable(this);
    }

    //region Serialization
    private static class DeleteTableEventBuilder implements ObjectBuilder<DeleteTableEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<DeleteTableEvent, DeleteTableEventBuilder> {
        @Override
        protected DeleteTableEventBuilder newBuilder() {
            return DeleteTableEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(DeleteTableEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.kvtName);
            target.writeLong(e.requestId);
            target.writeUUID(e.tableId);
        }

        private void read00(RevisionDataInput source, DeleteTableEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.kvtName(source.readUTF());
            b.requestId(source.readLong());
            b.tableId(source.readUUID());
        }
    }

    //endregion
}
