/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage.impl.hdfs;

import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;

/**
 * File descriptor for a segment file
 */
@ThreadSafe
class FileDescriptor {
    // region Members
    /**
     * The full HDFS path to this file.
     */
    @Getter
    private final Path path;

    /**
     * Segment offset of the first byte of this file. This is derived from the name.
     */
    @Getter
    private final long offset;

    /**
     * Epoch when the file was created. This is derived from the name.
     */
    @Getter
    private final long epoch;

    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean readOnly;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FileDescriptor class.
     *
     * @param path     The path of the file.
     * @param offset   The Segment Offset of the first byte in the file.
     * @param length   The length of the file.
     * @param epoch    The epoch the file was created in.
     * @param readOnly Whether the file is read-only.
     */
    FileDescriptor(Path path, long offset, long length, long epoch, boolean readOnly) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.epoch = epoch;
        this.readOnly = readOnly;
    }

    //endregion

    //region Properties

    /**
     * Updates the descriptor to indicate the file is read-only.
     * This does not change the underlying file.
     */
    synchronized void markReadOnly() {
        this.readOnly = true;
    }

    /**
     * Updates the descriptor to indicate the file is not read-only.
     * This does not change the underlying file.
     */
    synchronized void markReadWrite() {
        this.readOnly = false;
    }

    /**
     * Gets a value indicating whether the file is read-only or not.
     */
    synchronized boolean isReadOnly() {
        return this.readOnly;
    }

    /**
     * Increases the length of this file by the given amount.
     *
     * @param delta The amount to increase by.
     */
    synchronized void increaseLength(int delta) {
        Preconditions.checkState(!this.readOnly, "Cannot increase the length of a read-only file.");
        this.length += delta;
    }

    /**
     * Sets the length of the file to the given value.
     *
     * @param value The value to set.
     */
    synchronized void setLength(long value) {
        Preconditions.checkState(!this.readOnly, "Cannot change the length of a read-only file.");
        this.length = value;
    }

    /**
     * Gets a value indicating the length of this file. Invocations of
     *
     * @return The length of this file.
     */
    synchronized long getLength() {
        return this.length;
    }

    /**
     * Gets a value indicating the Segment offset corresponding to the last byte in this file.
     *
     * @return The result.
     */
    synchronized long getLastOffset() {
        return this.offset + this.length;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%d, %s)", this.path, this.length, this.readOnly ? "R" : "RW");
    }

    //endregion
}