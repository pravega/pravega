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

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Read Result Entry for data that is readily available for reading (in memory).
 */
class CacheReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at. NOTE: this is
     *                            not where the first byte of 'data' starts, rather it's where dataOffset points to in the
     *                            StreamSegment.
     * @param data                The data buffer that contains the data.
     * @param dataOffset          The offset within data where this ReadResultEntry starts at.
     * @param dataLength          The length of the data that this ReadResultEntry has.
     */
    CacheReadResultEntry(long streamSegmentOffset, byte[] data, int dataOffset, int dataLength) {
        super(ReadResultEntryType.Cache, streamSegmentOffset + dataOffset, dataLength);
        Exceptions.checkArrayRange(dataOffset, dataLength, data.length, "dataOffset", "dataLength");
        complete(new ReadResultEntryContents(new ByteArrayInputStream(data, dataOffset, dataLength), dataLength));
    }

    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at.
     * @param data                An InputStream representing the data to be read.
     * @param dataLength          The length of the data that this ReadResultEntry has (length of the given InputStream).
     */
    CacheReadResultEntry(long streamSegmentOffset, InputStream data, int dataLength) {
        super(ReadResultEntryType.Cache, streamSegmentOffset, dataLength);
        complete(new ReadResultEntryContents(data, dataLength));
    }
}
