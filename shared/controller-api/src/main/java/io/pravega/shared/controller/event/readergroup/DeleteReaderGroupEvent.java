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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class DeleteReaderGroupEvent implements ControllerEvent {
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
        return ((StreamRequestProcessor) processor).processDeleteReaderGroup(this);
    }

    //region Serialization
    private static class DeleteReaderGroupEventBuilder implements ObjectBuilder<DeleteReaderGroupEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<DeleteReaderGroupEvent, DeleteReaderGroupEventBuilder> {
        @Override
        protected DeleteReaderGroupEventBuilder newBuilder() {
            return DeleteReaderGroupEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(DeleteReaderGroupEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.kvtName);
            target.writeLong(e.requestId);
            target.writeUUID(e.tableId);
        }

        private void read00(RevisionDataInput source, DeleteReaderGroupEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.kvtName(source.readUTF());
            b.requestId(source.readLong());
            b.tableId(source.readUUID());
        }
    }

    //endregion
}
