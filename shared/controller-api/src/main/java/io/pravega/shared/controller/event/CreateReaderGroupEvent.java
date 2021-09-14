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

import com.google.common.collect.ImmutableMap;
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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class CreateReaderGroupEvent implements ControllerEvent {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;
    private final long requestId;
    private final String scope;
    private final String rgName;
    private final long groupRefreshTimeMillis;
    private final long automaticCheckpointIntervalMillis;
    private final int maxOutstandingCheckpointRequest;
    private final int retentionTypeOrdinal;
    private final long generation;
    private final UUID readerGroupId;
    private final Map<String, RGStreamCutRecord> startingStreamCuts;
    private final Map<String, RGStreamCutRecord> endingStreamCuts;
    private final long createTimeStamp;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, rgName);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((StreamRequestProcessor) processor).processCreateReaderGroup(this);
    }

    //region Serialization
    private static class CreateReaderGroupEventBuilder implements ObjectBuilder<CreateReaderGroupEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<CreateReaderGroupEvent, CreateReaderGroupEventBuilder> {
        @Override
        protected CreateReaderGroupEventBuilder newBuilder() {
            return CreateReaderGroupEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(CreateReaderGroupEvent e, RevisionDataOutput target) throws IOException {
            target.writeLong(e.requestId);
            target.writeUTF(e.scope);
            target.writeUTF(e.rgName);
            target.writeLong(e.createTimeStamp);
            target.writeLong(e.groupRefreshTimeMillis);
            target.writeLong(e.automaticCheckpointIntervalMillis);
            target.writeInt(e.maxOutstandingCheckpointRequest);
            target.writeCompactInt(e.retentionTypeOrdinal);
            target.writeLong(e.generation);
            target.writeUUID(e.readerGroupId);
            target.writeMap(e.startingStreamCuts, DataOutput::writeUTF, RGStreamCutRecord.SERIALIZER::serialize);
            target.writeMap(e.endingStreamCuts, DataOutput::writeUTF, RGStreamCutRecord.SERIALIZER::serialize);
        }

        private void read00(RevisionDataInput source, CreateReaderGroupEventBuilder eb) throws IOException {
            eb.requestId(source.readLong());
            eb.scope(source.readUTF());
            eb.rgName(source.readUTF());
            eb.createTimeStamp(source.readLong());
            eb.groupRefreshTimeMillis(source.readLong());
            eb.automaticCheckpointIntervalMillis(source.readLong());
            eb.maxOutstandingCheckpointRequest(source.readInt());
            eb.retentionTypeOrdinal(source.readCompactInt());
            eb.generation(source.readLong());
            eb.readerGroupId(source.readUUID());
            ImmutableMap.Builder<String, RGStreamCutRecord> startStreamCutBuilder = ImmutableMap.builder();
            source.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize, startStreamCutBuilder);
            eb.startingStreamCuts(startStreamCutBuilder.build());
            ImmutableMap.Builder<String, RGStreamCutRecord> endStreamCutBuilder = ImmutableMap.builder();
            source.readMap(DataInput::readUTF, RGStreamCutRecord.SERIALIZER::deserialize, endStreamCutBuilder);
            eb.endingStreamCuts(endStreamCutBuilder.build());
        }
    }
    //endregion
}
