/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@Builder
@Data
@AllArgsConstructor
public class CreateReaderGroupEvent implements ControllerEvent {
    private static final long serialVersionUID = 1L;
    private final String scopeName;
    private final String rgName;
    private final long requestId;

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
            target.writeLong(e.requestId);
        }

        private void read00(RevisionDataInput source, CreateReaderGroupEventBuilder eb) throws IOException {
            eb.scopeName(source.readUTF());
            eb.rgName(source.readUTF());
            eb.requestId(source.readLong());
        }
    }
    //endregion
}
