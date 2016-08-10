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

package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.SegmentProperties;

import java.util.Date;

/**
 * General Stream Segment Information.
 */
public class StreamSegmentInformation implements SegmentProperties {
    //region Members

    private final long length;
    private final boolean sealed;
    private final boolean deleted;
    private final Date lastModified;
    private final String streamSegmentName;

    //endregion

    //region Constructor

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
        this.length = length;
        this.sealed = isSealed;
        this.deleted = isDeleted;
        this.lastModified = lastModified;
        this.streamSegmentName = streamSegmentName;
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.streamSegmentName;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    @Override
    public boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public Date getLastModified() {
        return this.lastModified;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("Name = %s, Length = %d, Sealed = %s, Deleted = %s, LastModified = %s", getName(), getLength(), isSealed(), isDeleted(), getLastModified());
    }

    //endregion
}
