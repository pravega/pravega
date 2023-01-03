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
package io.pravega.client.stream;

import io.pravega.client.stream.impl.StreamCutInternal;
import io.pravega.common.util.ByteBufferUtils;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A set of segment/offset pairs for a single stream that represent a consistent position in the
 * stream. (IE: Segment 1 and 2 will not both appear in the set if 2 succeeds 1, and if 0 appears
 * and is responsible for keyspace 0-0.5 then other segments covering the range 0.5-1.0 will also be
 * included.)
 */
public interface StreamCut extends Serializable {

    /**
     * This is used represents an unbounded StreamCut. This is used when the user wants to refer to the current HEAD
     * of the stream or the current TAIL of the stream.
     */
    StreamCut UNBOUNDED = new StreamCut() {
        private static final long serialVersionUID = 1L;

        @Override
        public ByteBuffer toBytes() {
            return ByteBufferUtils.EMPTY;
        }

        @Override
        public String asText() {
            return toString();
        }

        @Override
        public StreamCutInternal asImpl() {
            return null;
        }

        @Override
        public String toString() {
            return "UNBOUNDED";
        }
        
        private Object readResolve() {
            return UNBOUNDED;
        }
    };
    
    /**
     * Used internally. Do not call.
     *
     * @return Implementation of EventPointer interface
     */
    StreamCutInternal asImpl();
    
    /**
     * Serializes the cut to a compact byte array.
     *
     * @return  A serialized version of this streamcut.
     */
    ByteBuffer toBytes();

    /**
     * Obtains the compact base64 string representation of StreamCut.
     *
     * @return Base64 representation of the StreamCut.
     */
    String asText();

    /**
     * Obtains the a StreamCut object from its Base64 representation obtained via {@link StreamCut#asText()}.
     *
     * @param base64String Base64 representation of StreamCut obtained using {@link StreamCut#asText()}
     * @return The StreamCut object
     */
    static StreamCut from(String base64String) {
        if (base64String.equals(UNBOUNDED.asText())) {
            return UNBOUNDED;
        }
        return StreamCutInternal.from(base64String);
    }

    /**
     * Deserializes the cut from its serialized from obtained from calling {@link #toBytes()}.
     *
     * @param cut A serialized position.
     * @return The StreamCut object.
     */
    static StreamCut fromBytes(ByteBuffer cut) {
        if (!cut.hasRemaining()) {
            return UNBOUNDED;
        }
        return StreamCutInternal.fromBytes(cut);
    }

}
