/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage;

/**
 * Represents the base for an addressing scheme inside a DurableDataLog. This can be used for accurately locating where
 * DataFrames are stored inside a DurableDataLog.
 *
 * Subclasses would be specific to DurableDataLog implementations.
 */
public abstract class LogAddress {
    private final long sequence;

    /**
     * Creates a new instance of the LogAddress class.
     * @param sequence The sequence of the address (location).
     */
    public LogAddress(long sequence) {
        this.sequence = sequence;
    }

    /**
     * Gets a value indicating the Sequence of the address (location).
     */
    public long getSequence() {
        return this.sequence;
    }

    @Override
    public String toString() {
        return String.format("Sequence = %d", this.sequence);
    }
}
