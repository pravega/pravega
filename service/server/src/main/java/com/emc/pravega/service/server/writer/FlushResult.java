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

import com.google.common.base.Preconditions;

/**
 * Represents the result of a Storage Flush Operation.
 */
class FlushResult {
    private final int length;

    FlushResult(int length) {
        Preconditions.checkArgument(length >= 0, "length must be a positive integer");
        this.length = length;
    }

    /**
     * Gets a value indicating the total amount of data flushed, in bytes.
     *
     * @return The result
     */
    public int getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return String.format("Length = %d", this.length);
    }
}
