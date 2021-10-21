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
 * Exception that is thrown whenever a segment of the wrong type is accessed (i.e., we want a StreamSegment but were given
 * the name of a Table Segment).
 */
public class BadSegmentTypeException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadSegmentTypeException class.
     *
     * @param segmentName  The name of the Segment.
     * @param expectedType The expected type for the Segment.
     * @param actualType   The actual type.
     */
    public BadSegmentTypeException(String segmentName, SegmentType expectedType, SegmentType actualType) {
        super(getMessage(segmentName, expectedType, actualType));
    }

    private static String getMessage(String segmentName, SegmentType expectedType, SegmentType actualType) {
        return String.format("Bad Segment Type for '%s'. Expected '%s', given '%s'.", segmentName, expectedType, actualType);
    }
}
