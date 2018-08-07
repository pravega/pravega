/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.stream.impl.StreamCutInternal;
import io.pravega.common.io.SerializationException;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static io.pravega.common.io.SerializationHelpers.deserializeBase64;
import static io.pravega.common.io.SerializationHelpers.serializeBase64;

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
    public static final StreamCut UNBOUNDED = new StreamCut() {
        private static final long serialVersionUID = 1L;
        
        @Override
        public ByteBuffer toBytes() {
            return ByteBuffer.allocate(0);
        }

        @Override
        public String asText() throws SerializationException {
            return serializeBase64(this);
        }

        @Override
        public StreamCutInternal asImpl() {
            return null;
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
     */
    ByteBuffer toBytes();

    /**
     * Serializes the cut to a compact base64 string representation.
     * @return Base64 representation of the StreamCut.
     * @throws SerializationException Exception during serialization.
     */
    String asText() throws SerializationException;
    
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

    /**
     * Deserializes a Base64 representation of the StreamCut obtained via {@link StreamCut#asText()}.
     * @param base64String Base64 representation of StreamCut obtained using {@link StreamCut#asText()}
     * @return The StreamCut object
     * @throws SerializationException Exception during deserialization.
     */
    static StreamCut of(String base64String) throws SerializationException {
        return deserializeBase64(base64String);
    }

}
