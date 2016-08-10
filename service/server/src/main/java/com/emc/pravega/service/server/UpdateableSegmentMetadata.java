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

import com.emc.pravega.service.contracts.AppendContext;

import java.util.Date;

/**
 * Defines an updateable StreamSegment Metadata.
 */
public interface UpdateableSegmentMetadata extends SegmentMetadata {
    /**
     * Sets the current StorageLength for this StreamSegment.
     *
     * @param value The StorageLength to set.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setStorageLength(long value);

    /**
     * Sets the current DurableLog Length for this StreamSegment.
     *
     * @param value
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setDurableLogLength(long value);

    /**
     * Marks this StreamSegment as sealed for modifications.
     */
    void markSealed();

    /**
     * Marks this StreamSegment as deleted.
     */
    void markDeleted();

    /**
     * Marks this StreamSegment as merged.
     */
    void markMerged();

    /**
     * Records the given Append Context and marks it as the one for the last committed Append Context.
     *
     * @param appendContext The AppendContext to record.
     */
    void recordAppendContext(AppendContext appendContext);

    /**
     * Sets the Last Modified date.
     * @param date
     */
    void setLastModified(Date date);

    /**
     * Updates this instance of the UpdateableSegmentMetadata to have the same information as the other one.
     *
     * @param other The SegmentMetadata to copy from.
     * @throws IllegalArgumentException If the other SegmentMetadata refers to a different StreamSegment.
     */
    void copyFrom(SegmentMetadata other);
}