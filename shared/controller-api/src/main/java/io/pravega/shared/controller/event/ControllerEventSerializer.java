/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.event;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.SneakyThrows;

/**
 * {@link ControllerEvent} serializer.
 */
public class ControllerEventSerializer extends VersionedSerializer.MultiType<ControllerEvent> {
    @Override
    protected void declareSerializers(Builder builder) {
        // Unused values (Do not repurpose!):
        // - 0: Unsupported Serializer.
        builder.serializer(AbortEvent.class, 1, new AbortEvent.Serializer())
               .serializer(AutoScaleEvent.class, 2, new AutoScaleEvent.Serializer())
               .serializer(CommitEvent.class, 3, new CommitEvent.Serializer())
               .serializer(DeleteStreamEvent.class, 4, new DeleteStreamEvent.Serializer())
               .serializer(ScaleOpEvent.class, 5, new ScaleOpEvent.Serializer())
               .serializer(SealStreamEvent.class, 6, new SealStreamEvent.Serializer())
               .serializer(TruncateStreamEvent.class, 7, new TruncateStreamEvent.Serializer())
               .serializer(UpdateStreamEvent.class, 8, new UpdateStreamEvent.Serializer());
    }

    /**
     * Serializes the given {@link ControllerEvent} to a {@link ByteBuffer}.
     *
     * @param value The {@link ControllerEvent} to serialize.
     * @return A new {@link ByteBuffer} wrapping an array that contains the serialization.
     */
    @SneakyThrows(IOException.class)
    public ByteBuffer toByteBuffer(ControllerEvent value) {
        ByteArraySegment s = serialize(value);
        return ByteBuffer.wrap(s.array(), s.arrayOffset(), s.getLength());
    }

    /**
     * Deserializes the given {@link ByteBuffer} into a {@link ControllerEvent} instance.
     *
     * @param buffer {@link ByteBuffer} to deserialize.
     * @return A new {@link ControllerEvent} instance from the given serialization.
     */
    @SneakyThrows(IOException.class)
    public ControllerEvent fromByteBuffer(ByteBuffer buffer) {
        return deserialize(new ByteArraySegment(buffer));
    }
}
