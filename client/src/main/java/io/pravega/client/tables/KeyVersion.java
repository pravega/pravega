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
package io.pravega.client.tables;

import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import java.io.Serializable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Version of a Key in a Table.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class KeyVersion implements Serializable {
    /**
     * {@link KeyVersion} that indicates no specific version is desired. Using this will result in an unconditional
     * update or removal being performed. See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    public static final KeyVersion NO_VERSION = new KeyVersion(null, TableSegmentKeyVersion.NO_VERSION);
    /**
     * {@link KeyVersion} that indicates the {@link TableKey} must not exist. Using this will result in an conditional
     * update or removal being performed, conditioned on the {@link TableKey} not existing at the time of the operation.
     * See {@link KeyValueTable} for details on conditional/unconditional updates.
     */
    public static final KeyVersion NOT_EXISTS = new KeyVersion(null, TableSegmentKeyVersion.NOT_EXISTS);

    /**
     * The Segment where this Key resides. May be null if this is a {@link #NOT_EXISTS} or {@link #NO_VERSION}
     * {@link KeyVersion}.
     */
    private final String segmentName;
    /**
     * The internal version inside the Table Segment for this Key.
     */
    private final TableSegmentKeyVersion segmentVersion;

    /**
     * Creates a new instance of the {@link KeyVersion} class.
     *
     * @param segmentName    The name of the Table Segment that contains the {@link TableKey}.
     * @param segmentVersion The version within the Table Segment for the {@link TableKey}.
     */
    KeyVersion(String segmentName, long segmentVersion) {
        this.segmentName = segmentName;
        this.segmentVersion = TableSegmentKeyVersion.from(segmentVersion);
    }

    /**
     * The internal version inside the Table Segment for this Key.
     *
     * @return The Segment Version
     */
    long getSegmentVersion() {
        return this.segmentVersion.getSegmentVersion();
    }
}
