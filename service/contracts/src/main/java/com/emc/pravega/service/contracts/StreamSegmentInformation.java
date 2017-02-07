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

import java.util.Date;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Data;
import lombok.Getter;

/**
 * General Stream Segment Information.
 */
public class StreamSegmentInformation implements SegmentProperties {

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
    private final Map<UUID, Long> attributes;

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param lastModified      The last time the StreamSegment was modified.
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Date lastModified) {
        this.name = streamSegmentName;
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
        this.attributes = new HashMap<>(); //TODO: populate.
    }

    @Override
    public long getAttributeValue(UUID attributeId, long defaultValue) {
        return this.attributes.getOrDefault(attributeId, defaultValue);
    }

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }
}
