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
package io.pravega.shared.controller.event;

import com.google.common.collect.ImmutableSet;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class UpdateReaderGroupEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String rgName;
    private final long requestId;
    private UUID readerGroupId;
    private long generation;
    private boolean isTransitionToFromSubscriber;
    private ImmutableSet<String> removeStreams;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, rgName);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((StreamRequestProcessor) processor).processUpdateReaderGroup(this);
    }

    //region Serialization
    private static class UpdateReaderGroupEventBuilder implements ObjectBuilder<UpdateReaderGroupEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<UpdateReaderGroupEvent, UpdateReaderGroupEventBuilder> {
        @Override
        protected UpdateReaderGroupEventBuilder newBuilder() {
            return UpdateReaderGroupEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(UpdateReaderGroupEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.rgName);
            target.writeLong(e.requestId);
            target.writeUUID(e.readerGroupId);
            target.writeLong(e.generation);
            target.writeBoolean(e.isTransitionToFromSubscriber);
            target.writeCollection(e.removeStreams, DataOutput::writeUTF);
        }

        private void read00(RevisionDataInput source, UpdateReaderGroupEventBuilder eb) throws IOException {
            eb.scope(source.readUTF());
            eb.rgName(source.readUTF());
            eb.requestId(source.readLong());
            eb.readerGroupId(source.readUUID());
            eb.generation(source.readLong());
            eb.isTransitionToFromSubscriber(source.readBoolean());
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            source.readCollection(DataInput::readUTF, builder);
            eb.removeStreams(builder.build());
        }
    }
    //endregion
}
