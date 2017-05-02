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
package io.pravega.service.contracts;

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
     * @param givenOffset       The offset that was given as part of the operation.
     */
    public BadOffsetException(String streamSegmentName, long expectedOffset, long givenOffset) {
        super(streamSegmentName, getMessage(expectedOffset, givenOffset));
    }

    private static String getMessage(long expectedOffset, long givenOffset) {
        return String.format("Bad Offset. Expected %d, given %d.", expectedOffset, givenOffset);
    }
}
