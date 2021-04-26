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
package io.pravega.segmentstore.contracts;

/**
 * An Exception that is related to a particular Container.
 */
public abstract class StreamSegmentException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final String streamSegmentName;

    /**
     * Creates a new instance of the StreamSegmentException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param message           The message for this exception.
     */
    public StreamSegmentException(String streamSegmentName, String message) {
        this(streamSegmentName, message, null);
    }

    /**
     * Creates a new instance of the StreamSegmentException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param message           The message for this exception.
     * @param cause             The causing exception.
     */
    public StreamSegmentException(String streamSegmentName, String message, Throwable cause) {
        super(String.format("[Segment '%s'] %s", streamSegmentName, message), cause);
        this.streamSegmentName = streamSegmentName;
    }

    /**
     * Gets a value indicating the StreamSegment Name.
     *
     * @return the name of the Stream Segment
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }
}
