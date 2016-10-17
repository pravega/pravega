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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.function.CallbackHelpers;
import com.google.common.base.Preconditions;

import java.io.InputStream;
import java.util.function.BiConsumer;

/**
 * Validates that a stream of data is made up of appends generated using AppendContentGenerator.
 */
class DataValidator {
    //region Members

    private final CircularArray buffer;
    private long bufferSegmentOffset;
    private final BiConsumer<Long, AppendContentGenerator.ValidationResult> failedValidationCallback;
    private final Object lock = new Object();

    //endregion

    // region Constructor

    DataValidator(int maxBufferSize, BiConsumer<Long, AppendContentGenerator.ValidationResult> failedValidationCallback) {
        Preconditions.checkNotNull(failedValidationCallback, "failedValidationCallback");
        this.buffer = new CircularArray(maxBufferSize);
        this.bufferSegmentOffset = -1;
        this.failedValidationCallback = failedValidationCallback;
    }

    //endregion

    //region Validation

    /**
     * Processes the given InputStream.
     *
     * @param data          The InputStream (data) to process.
     * @param segmentOffset The offset within the Segment where the given InputStream starts at.
     * @param length        The length of the InputStream.
     */
    void process(InputStream data, long segmentOffset, int length) {
        synchronized (this.lock) {
            // Verify that append data blocks are contiguous.
            if (this.bufferSegmentOffset >= 0) {
                Preconditions.checkArgument(segmentOffset == this.bufferSegmentOffset + this.buffer.getLength());
            } else {
                this.bufferSegmentOffset = segmentOffset;
            }

            // Append data to buffer.
            this.buffer.append(data, length);
        }

        // Trigger validation (this doesn't necessarily mean validation will happen, though).
        triggerValidation();
    }

    private void triggerValidation() {
        synchronized (this.lock) {
            // Repeatedly validate the contents of the buffer, until we found a non-successful result or until it is completely drained.
            AppendContentGenerator.ValidationResult validationResult;
            do {
                // Validate the tip of the buffer.
                validationResult = AppendContentGenerator.validate(this.buffer, 0);
                if (validationResult.isFailed()) {
                    // Validation failed. Invoke callback.
                    CallbackHelpers.invokeSafely(this.failedValidationCallback, this.bufferSegmentOffset, validationResult, null);
                } else if (!validationResult.isMoreDataNeeded()) {
                    // Validation did not fail and no more data is needed; free up the buffer and move on.
                    this.buffer.truncate(validationResult.getLength());
                    this.bufferSegmentOffset += validationResult.getLength();
                }
            } while (validationResult.isSuccess() && this.buffer.getLength() > 0);
        }
    }

    //endregion
}
