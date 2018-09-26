/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.nio.ByteBuffer;

/**
 * Version of a Key in a Table.
 */
public interface KeyVersion {
    /**
     * A special KeyVersion which indicates the Key must not exist when performing Conditional Updates.
     */
    KeyVersion NOT_EXISTS = null; // TODO: replace with actual implementation instance once it exists.

    /**
     * Gets a value representing the internal Table Segment Id where this Key Version refers to.
     */
    long getSegmentId();

    /**
     * Gets a value representing the internal version inside the Table Segment for this Key.
     */
    long getSegmentVersion();

    /**
     * Serializes the KeyVersion instance to a compact byte array.
     */
    ByteBuffer toBytes();

    /**
     * Deserializes the KeyVersion from its serialized from obtained from calling {@link #toBytes()}.
     *
     * @param serializedKeyVersion A serialized KeyVersion.
     * @return The KeyVersion object.
     */
    static KeyVersion fromBytes(ByteBuffer serializedKeyVersion) {
        throw new UnsupportedOperationException("Not Implemented");
    }
}
