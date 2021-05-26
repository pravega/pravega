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
package io.pravega.client.segment.impl;

import java.io.IOException;

/**
 * A segment has ended. No more events may be read from it.
 * This exception is thrown when SegmentInputStream reaches
 *    - end of a {@link Segment}
 *    - configured end offset of a {@link Segment}.
 */
public class EndOfSegmentException extends IOException {

    private static final long serialVersionUID = 1L;

    private final ErrorType errorType;

    public EndOfSegmentException() {
        this.errorType = ErrorType.END_OF_SEGMENT_REACHED;
    }

    public EndOfSegmentException(ErrorType cause) {
        this.errorType = cause;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public enum ErrorType {
        END_OF_SEGMENT_REACHED,
        END_OFFSET_REACHED
    }
}
