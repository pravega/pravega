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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.tables.IteratorState;
import java.nio.ByteBuffer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * {@link IteratorState} Implementation.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class IteratorStateImpl implements IteratorState {
    /**
     * No state. Providing this value will result in an iterator that starts from the beginning (i.e., not resuming an
     * existing iteration).
     */
    public static final IteratorStateImpl EMPTY = new IteratorStateImpl(Unpooled.EMPTY_BUFFER);
    private final ByteBuf token;

    @Override
    public ByteBuffer toBytes() {
        return this.token.copy().nioBuffer();
    }

    @Override
    public boolean isEmpty() {
        return this.token.readableBytes() == 0;
    }

    /**
     * Deserializes the {@link IteratorStateImpl} from its serialized form obtained from calling {@link #getToken()} .
     *
     * @param serializedState A serialized {@link IteratorStateImpl}.
     * @return The IteratorState object.
     */
    public static IteratorStateImpl fromBytes(ByteBuf serializedState) {
        if (serializedState == null || serializedState.readableBytes() == 0) {
            return EMPTY;
        } else {
            return new IteratorStateImpl(serializedState);
        }
    }

    /**
     * Deserializes the IteratorState from its serialized form obtained from calling {@link #toBytes()} ()} .
     *
     * @param serializedState A serialized {@link IteratorStateImpl}.
     * @return The IteratorState object.
     */
    public static IteratorStateImpl fromBytes(ByteBuffer serializedState) {
        if (serializedState == null) {
            return EMPTY;
        } else {
            return IteratorStateImpl.fromBytes(Unpooled.wrappedBuffer(serializedState));
        }
    }

    /**
     * Creates a new {@link IteratorStateImpl} which is a copy of the given {@link IteratorState}.
     *
     * @param source The {@link IteratorState} to copy.
     * @return The copy.
     */
    public static IteratorStateImpl copyOf(IteratorState source) {
        if (source == null || source == IteratorStateImpl.EMPTY) {
            return EMPTY;
        }

        return fromBytes(source.toBytes());
    }
}
