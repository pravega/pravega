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

import lombok.Data;

/**
 * General Stream Segment Information.
 */
@Data
public class StreamSegmentInformation implements SegmentProperties {

    private final String name;
    private final long length;
    private final boolean sealed;
    private final boolean deleted;
    private final Date lastModified;
    private final boolean autoScale;
    private final long targetRate;
    private final byte rateType;

    /**
     * Creates a new instance of the StreamSegmentInformation class.
     *  @param streamSegmentName The name of the StreamSegment.
     * @param length            The length of the StreamSegment.
     * @param isSealed          Whether the StreamSegment is sealed (for modifications).
     * @param isDeleted         Whether the StreamSegment is deleted (does not exist).
     * @param lastModified      The last time the StreamSegment was modified.
     * @param autoScale         Whether autoscaling is enabled for the stream
     * @param targetRate        If autoscaling is enabled, what is the desired rate
     * @param rateType          Type of desired rate in bytes or number of events
     */
    public StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Date lastModified, boolean autoScale, long targetRate, byte rateType) {
        this.name = streamSegmentName;
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
        this.autoScale = autoScale;
        this.targetRate = targetRate;
        this.rateType = rateType;
    }

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

}
