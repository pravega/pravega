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

package com.emc.logservice.server.reading;

/**
 * A ReadIndexEntry that has actual data in memory.
 */
public class ByteArrayReadIndexEntry extends ReadIndexEntry {
    //region Members

    private final byte[] data;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @param data                The data.
     * @throws NullPointerException     If data is null.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    protected ByteArrayReadIndexEntry(long streamSegmentOffset, byte[] data) {
        super(streamSegmentOffset, data.length);
        this.data = data;
    }

    //endregion

    //region Properties

    /**
     * Gets a byte array containing the data for this entry.
     *
     * @return
     */
    public byte[] getData() {
        return this.data;
    }

    //endregion
}
