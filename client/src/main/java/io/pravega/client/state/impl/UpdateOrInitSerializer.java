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
package io.pravega.client.state.impl;

import io.pravega.client.state.Revisioned;
import io.pravega.client.state.Update;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.stream.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class UpdateOrInitSerializer<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
        implements Serializer<UpdateOrInit<StateT>>, Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final int INITIALIZATION = 1;
    private static final int UPDATE = 2;
    private final Serializer<UpdateT> updateSerializer;
    private final Serializer<InitT> initSerializer;

    @Override
    @SuppressWarnings("unchecked")
    public ByteBuffer serialize(UpdateOrInit<StateT> value) {
        if (value.isInit()) {
            ByteBuffer buffer = initSerializer.serialize((InitT) value.getInit());
            ByteBuffer result = ByteBuffer.allocate(buffer.capacity() + Integer.BYTES );
            result.putInt(INITIALIZATION);
            result.put(buffer);
            result.rewind();
            return result;
        } else {
            List<ByteBuffer> serializedUpdates = new ArrayList<>();
            int size = 0;
            for (Update<StateT> u : value.getUpdates()) {
                ByteBuffer serialized = updateSerializer.serialize((UpdateT) u);
                size += serialized.remaining();
                serializedUpdates.add(serialized);
            }
            ByteBuffer result = ByteBuffer.allocate(size + Integer.BYTES + serializedUpdates.size() * Integer.BYTES);
            result.putInt(UPDATE);
            for (ByteBuffer update : serializedUpdates) {
                result.putInt(update.remaining());
                result.put(update);
            }
            result.rewind();
            return result;
        }
    }

    @Override
    public UpdateOrInit<StateT> deserialize(ByteBuffer serializedValue) {
        int type = serializedValue.getInt();
        if (type == INITIALIZATION) {
            return new UpdateOrInit<>(initSerializer.deserialize(serializedValue));
        } else if (type == UPDATE) {
            ArrayList<Update<StateT>> result = new ArrayList<>();
            int origionalLimit = serializedValue.limit();
            while (serializedValue.hasRemaining()) {
                int updateLength = serializedValue.getInt();
                int position = serializedValue.position();
                serializedValue.limit(serializedValue.position() + updateLength);
                result.add(updateSerializer.deserialize(serializedValue));
                serializedValue.limit(origionalLimit);
                serializedValue.position(position + updateLength);
            }
            return new UpdateOrInit<>(result);
        } else {
            throw new CorruptedStateException("Update of unknown type: " + type);
        }
    }
}