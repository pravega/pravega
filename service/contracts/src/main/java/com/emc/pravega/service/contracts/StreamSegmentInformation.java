/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.contracts;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

/**
 * General Stream Segment Information.
 */
public class StreamSegmentInformation implements SegmentProperties {
    //region Members

    @Getter
    private final String name;
    @Getter
    private final long length;
    @Getter
    private final boolean sealed;
    @Getter
    private final boolean deleted;
    @Getter
    private final Date lastModified;
    @Getter
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentInformation class with no attributes.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param lastModified      The last time the StreamSegment was modified.
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Date lastModified) {
        this(streamSegmentName, length, isSealed, isDeleted, null, lastModified);
    }

    /**
     * Creates a new instance of the StreamSegmentInformation class with attributes.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param attributes        The attributes of this StreamSegment.
     * @param lastModified      The last time the StreamSegment was modified.
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Map<UUID, Long> attributes, Date lastModified) {
        this.name = streamSegmentName;
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
        this.attributes = getAttributes(attributes);
    }

    /**
     * Creates a new instance of the StreamSegmentInformation class from a base SegmentProperties with replacement attributes.
     *
     * @param baseProperties The SegmentProperties to copy. Attributes will be ignored.
     * @param attributes     The attributes of this StreamSegment.
     */
    public StreamSegmentInformation(SegmentProperties baseProperties, Map<UUID, Long> attributes) {
        this.name = baseProperties.getName();
        this.length = baseProperties.getLength();
        this.sealed = baseProperties.isSealed();
        this.deleted = baseProperties.isDeleted();
        this.lastModified = baseProperties.getLastModified();
        this.attributes = getAttributes(attributes);
    }

    //endregion

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

    private static Map<UUID, Long> getAttributes(Map<UUID, Long> input) {
        return input == null ? Collections.emptyMap() : Collections.unmodifiableMap(input);
        return input == null || input.size() == 0 ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(input));
    }
}
