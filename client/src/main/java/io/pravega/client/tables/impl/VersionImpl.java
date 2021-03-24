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

import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.Version;
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
public class VersionImpl implements Version, Serializable {
    /**
     * Denotes the lack of any Segment.
     */
    public static final long NO_SEGMENT_ID = Long.MIN_VALUE;
    /**
     * The Segment where this Key resides. May equal {@link #NO_SEGMENT_ID}if this is a {@link #NOT_EXISTS} or
     * {@link #NO_VERSION} {@link Version}.
     */
    @Getter(AccessLevel.PACKAGE)
    private final long segmentId;
    /**
     * The internal version inside the Table Segment for this Key.
     */
    private final TableSegmentKeyVersion segmentVersion;

    /**
     * Creates a new instance of the {@link Version} class.
     *
     * @param segmentId      The internal id of the Table Segment that contains the {@link TableKey}.
     * @param segmentVersion The version within the Table Segment for the {@link TableKey}.
     */
    VersionImpl(long segmentId, long segmentVersion) {
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
    public VersionImpl asImpl() {
        return this;
    }

    @Override
    public String toString() {
        return String.format("%d:%d", this.segmentId, getSegmentVersion());
    }

    /**
     * Deserializes the {@link VersionImpl} from its serialized form obtained from calling {@link #toString()}.
     *
     * @param str A serialized {@link VersionImpl}.
     * @return The {@link VersionImpl} object.
     */
    public static VersionImpl fromString(String str) {
        String[] tokens = str.split(":");
        if (tokens.length == 2) {
            return new VersionImpl(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]));
        }

        throw new SerializationException(String.format("Not a valid KeyVersion serialization: '%s'.", str));
    }
}