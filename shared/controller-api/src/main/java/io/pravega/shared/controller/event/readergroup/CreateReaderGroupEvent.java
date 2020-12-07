/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event.readergroup;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.shared.controller.event.StreamRequestProcessor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class CreateReaderGroupEvent implements ControllerEvent {
    private static final long serialVersionUID = 1L;
    private final String scopeName;
    private final String rgName;
    private final long timestamp;
    private final long requestId;
    private final UUID readerGroupId;

    @Override
    public String getKey() {
        return String.format("%s/%s", scopeName, rgName);
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
            target.writeUTF(e.scopeName);
            target.writeUTF(e.rgName);
            target.writeLong(e.timestamp);
            target.writeLong(e.requestId);


        }

        private void read00(RevisionDataInput source, CreateTableEventBuilder eb) throws IOException {
            eb.scopeName(source.readUTF());
            eb.kvtName(source.readUTF());
            eb.partitionCount(source.readInt());
            eb.timestamp(source.readLong());
            eb.requestId(source.readLong());
            eb.tableId(source.readUUID());
        }
    }
    //endregion
}
