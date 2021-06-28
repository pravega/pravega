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
import io.pravega.client.tables.IteratorItem;
import java.nio.ByteBuffer;
import java.util.List;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * {@link IteratorItem} implementation for Hash Table Segments (used internally by Pravega to store metadata).
 *
 * Note: This class is kept in the Client for backward compatibility/historical reasons only. It should not be used with
 * {@link io.pravega.client.tables.KeyValueTable} instances (use the {@link IteratorItem} with that instead.
 * The Controller uses this internally to perform iterations over its table segments. Eventually, this should be integrated
 * into the {@link TableSegmentImpl} class when https://github.com/pravega/pravega/issues/4647 is implemented.
 *
 * @param <T> Iterator Item Type.
 */
@EqualsAndHashCode(callSuper = true)
public class HashTableIteratorItem<T> extends IteratorItem<T> {
    @Getter
    private final State state;

    /**
     * Creates a new instance of the {@link HashTableIteratorItem} class.
     *
     * @param state The Iterator {@link State}.
     * @param items The List of Items that are contained in this instance.
     */
    public HashTableIteratorItem(State state, List<T> items) {
        super(items);
        this.state = state;
    }

    /**
     * Iterator State.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @EqualsAndHashCode
    @Getter
    public static class State {
        /**
         * No state. Providing this value will result in an iterator that starts from the beginning (i.e., not resuming an
         * existing iteration).
         */
        public static final State EMPTY = new State(Unpooled.EMPTY_BUFFER);
        private final ByteBuf token;

        /**
         * Serializes this {@link State} into a {@link ByteBuffer}.
         *
         * @return A {@link ByteBuffer} representing the contents of this state.
         */
        public ByteBuffer toBytes() {
            return this.token.copy().nioBuffer();
        }

        /**
         * Gets a value indicating whether this {@link State} is empty (no state) or not.
         *
         * @return True if empty (no state), false otherwise.
         */
        public boolean isEmpty() {
            return this.token.readableBytes() == 0;
        }

        /**
         * Deserializes the {@link State} from its serialized form obtained from calling {@link #getToken()}.
         *
         * @param serializedState A serialized {@link State}.
         * @return The IteratorState object.
         */
        public static State fromBytes(ByteBuf serializedState) {
            if (serializedState == null || serializedState.readableBytes() == 0) {
                return EMPTY;
            } else {
                return new State(serializedState);
            }
        }

        /**
         * Deserializes the IteratorState from its serialized form obtained from calling {@link #toBytes()} .
         *
         * @param serializedState A serialized {@link State}.
         * @return The IteratorState object.
         */
        public static State fromBytes(ByteBuffer serializedState) {
            if (serializedState == null) {
                return EMPTY;
            } else {
                return State.fromBytes(Unpooled.wrappedBuffer(serializedState));
            }
        }

        /**
         * Creates a new {@link State} which is a copy of the given {@link State}.
         *
         * @param source The {@link State} to copy.
         * @return The copy.
         */
        public static State copyOf(State source) {
            if (source == null || source == State.EMPTY) {
                return EMPTY;
            }

            return fromBytes(source.toBytes());
        }
    }
}
