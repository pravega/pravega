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

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.contracts.ReadResultEntryType;

/**
 * Read Result Entry for data that is not yet available in the StreamSegment (for an offset that is beyond the
 * StreamSegment's DurableLogLength)
 */
class FutureReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the FutureReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @throws IllegalArgumentException If type is not ReadResultEntryType.Future or ReadResultEntryType.Storage.
     */
    FutureReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(ReadResultEntryType.Future, streamSegmentOffset, requestedReadLength);
    }
}
