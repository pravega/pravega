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

package com.emc.pravega.service.contracts;

/**
 * Exception that is thrown whenever a Write failed due to a bad offset.
 */
public class BadOffsetException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadOffsetException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param expectedOffset    The expected offset for the Operation.
     * @param actualOffset      The real offset (length) of the StreamSegment.
     */
    public BadOffsetException(String streamSegmentName, long expectedOffset, long actualOffset) {
        super(streamSegmentName, getMessage(expectedOffset, actualOffset));
    }

    private static String getMessage(long expectedOffset, long actualOffset) {
        return String.format("Bad Offset. Expected %d, Given %d.", expectedOffset, actualOffset);
    }
}
