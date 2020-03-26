/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.tables.KeyVersion;
import io.pravega.client.tables.TableKey;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.SerializationException;

/**
 * Version of a Key in a Table.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
public class KeyVersionImpl implements KeyVersion, Serializable {
    /**
     * Denotes the lack of any Segment.
     */
    public static final long NO_SEGMENT_ID = Long.MIN_VALUE;
    /**
     * The Segment where this Key resides. May equal {@link #NO_SEGMENT_ID}if this is a {@link #NOT_EXISTS} or
     * {@link #NO_VERSION} {@link io.pravega.client.tables.KeyVersion}.
     */
    @Getter(AccessLevel.PACKAGE)
    private final long segmentId;
    /**
     * The internal version inside the Table Segment for this Key.
     */
    private final TableSegmentKeyVersion segmentVersion;

    /**
     * Creates a new instance of the {@link io.pravega.client.tables.KeyVersion} class.
     *
     * @param segmentId      The internal id of the Table Segment that contains the {@link TableKey}.
     * @param segmentVersion The version within the Table Segment for the {@link TableKey}.
     */
    KeyVersionImpl(long segmentId, long segmentVersion) {
        this(segmentId, TableSegmentKeyVersion.from(segmentVersion));
    }

    /**
     * The internal version inside the Table Segment for this Key.
     *
     * @return The Segment Version
     */
    public long getSegmentVersion() {
        return this.segmentVersion.getSegmentVersion();
    }

    @Override
    public KeyVersionImpl asImpl() {
        return this;
    }

    @Override
    public String toString() {
        return String.format("%d:%d", this.segmentId, getSegmentVersion());
    }

    /**
     * Deserializes the {@link KeyVersionImpl} from its serialized form obtained from calling {@link #toString()}.
     *
     * @param str A serialized {@link KeyVersionImpl}.
     * @return The {@link KeyVersionImpl} object.
     */
    public static KeyVersionImpl fromString(String str) {
        String[] tokens = str.split(":");
        if (tokens.length == 2) {
            return new KeyVersionImpl(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]));
        }

        throw new SerializationException(String.format("Not a valid KeyVersion serialization: '%s'.", str));
    }
}