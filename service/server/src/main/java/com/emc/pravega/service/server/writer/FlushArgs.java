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

package com.emc.pravega.service.server.writer;

import com.google.common.collect.Iterators;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;

/**
 * Represents a set of arguments for a Storage Flush Operation.
 */
class FlushArgs {
    private final ArrayList<InputStream> streams;
    private int totalLength;

    /**
     * Creates a new instance of the FlushArgs class.
     */
    FlushArgs() {
        this.streams = new ArrayList<>();
        this.totalLength = 0;
    }

    /**
     * Incorporates the given byte array into the args.
     *
     * @param data The byte array to incorporate.
     */
    void add(byte[] data) {
        if (data.length > 0) {
            this.streams.add(new ByteArrayInputStream(data));
            this.totalLength += data.length;
        }
    }

    /**
     * Gets a new InputStream that represents all the added byte arrays (via the add() method), in the order in which
     * they were added.
     *
     * @return The result.
     */
    InputStream getInputStream() {
        return new SequenceInputStream(Iterators.asEnumeration(this.streams.iterator()));
    }

    /**
     * Gets a value indicating the total length of the data that was added.
     *
     * @return The result.
     */
    int getTotalLength() {
        return this.totalLength;
    }

    /**
     * Gets a value indicating the total number of byte arrays added via the add() method.
     *
     * @return The result.
     */
    int getCount() {
        return this.streams.size();
    }

    @Override
    public String toString() {
        return String.format("Count = %d, TotalSize = %d", this.streams.size(), this.totalLength);
    }
}
