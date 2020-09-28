/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
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
public class RemoveSubscriberEvent implements ControllerEvent {
    private static final long serialVersionUID = 1L;
    private final String scope;
    private final String stream;
    private final String subscriber;
    private final long requestId;

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }

    @Override
    public CompletableFuture<Void> process(RequestProcessor processor) {
        return ((StreamRequestProcessor) processor).processRemoveSubscriberStream(this);
    }

    //region Serialization
    private static class RemoveSubscriberEventBuilder implements ObjectBuilder<RemoveSubscriberEvent> {
    }

    public static class Serializer extends VersionedSerializer.WithBuilder<RemoveSubscriberEvent, RemoveSubscriberEventBuilder> {
        @Override
        protected RemoveSubscriberEventBuilder newBuilder() {
            return RemoveSubscriberEvent.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(RemoveSubscriberEvent e, RevisionDataOutput target) throws IOException {
            target.writeUTF(e.scope);
            target.writeUTF(e.stream);
            target.writeUTF(e.subscriber);
            target.writeLong(e.requestId);
        }

        private void read00(RevisionDataInput source, RemoveSubscriberEventBuilder b) throws IOException {
            b.scope(source.readUTF());
            b.stream(source.readUTF());
            b.subscriber(source.readUTF());
            b.requestId(source.readLong());
        }
    }

    //endregion
}
