/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class UpdateOrInitSerializer<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
        implements Serializer<UpdateOrInit<StateT>> {
    private static final int INITIALIZATION = 1;
    private static final int UPDATE = 2;
    private final Segment segment;
    private final Serializer<UpdateT> updateSerializer;
    private final Serializer<InitT> initSerializer;

    @Override
    @SuppressWarnings("unchecked")
    public ByteBuffer serialize(UpdateOrInit<StateT> value) {
        if (value.isInit()) {
            ByteBuffer buffer = initSerializer.serialize((InitT) value.getInit());
            ByteBuffer result = ByteBuffer.allocate(buffer.capacity() + 3 * Integer.BYTES + Long.BYTES);
            result.putInt(INITIALIZATION);
            result.putInt(value.getInitRevision().asImpl().getSegment().getSegmentNumber());
            result.putLong(value.getInitRevision().asImpl().getOffsetInSegment());
            result.putInt(value.getInitRevision().asImpl().getEventAtOffset());
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
            int segmentNumber = serializedValue.getInt();
            if (segmentNumber != segment.getSegmentNumber()) {
                throw new CorruptedStateException(
                        "Mismatched segment numbers. Was reading from: " + segment + " but read " + segmentNumber);
            }
            RevisionImpl revision = new RevisionImpl(segment, serializedValue.getLong(), serializedValue.getInt());
            return new UpdateOrInit<>(initSerializer.deserialize(serializedValue), revision);
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