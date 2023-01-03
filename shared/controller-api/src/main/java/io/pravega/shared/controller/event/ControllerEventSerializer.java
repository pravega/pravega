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

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
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
               .serializer(UpdateStreamEvent.class, 8, new UpdateStreamEvent.Serializer())
               .serializer(CreateTableEvent.class, 9, new CreateTableEvent.Serializer())
               .serializer(DeleteTableEvent.class, 10, new DeleteTableEvent.Serializer())
               .serializer(CreateReaderGroupEvent.class, 11, new CreateReaderGroupEvent.Serializer())
               .serializer(DeleteReaderGroupEvent.class, 12, new DeleteReaderGroupEvent.Serializer())
               .serializer(UpdateReaderGroupEvent.class, 13, new UpdateReaderGroupEvent.Serializer())
               .serializer(DeleteScopeEvent.class, 14, new DeleteScopeEvent.Serializer());
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
