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
package io.pravega.controller.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.Serializer;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import java.nio.ByteBuffer;
import lombok.NonNull;

/**
 * Wrapper for {@link ControllerEventSerializer} that implements {@link Serializer}.
 *
 * @param <T> Type of {@link ControllerEvent} to serialize/deserialize.
 */
public class EventSerializer<T extends ControllerEvent> implements Serializer<T> {
    private final ControllerEventSerializer baseSerializer;

    /**
     * Creates a new instance of the {@link EventSerializer} class.
     */
    public EventSerializer() {
        this(new ControllerEventSerializer());
    }

    /**
     * Creates a new instance of the {@link EventSerializer} class.
     *
     * @param baseSerializer The {@link ControllerEventSerializer} to use.
     */
    @VisibleForTesting
    public EventSerializer(@NonNull ControllerEventSerializer baseSerializer) {
        this.baseSerializer = baseSerializer;
    }

    @Override
    public ByteBuffer serialize(T value) {
        return this.baseSerializer.toByteBuffer(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(ByteBuffer serializedValue) {
        return (T) this.baseSerializer.fromByteBuffer(serializedValue);
    }
}
