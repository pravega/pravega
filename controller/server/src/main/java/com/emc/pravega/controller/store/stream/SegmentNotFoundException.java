/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

/**
 * Exception thrown when a segment with a given name is not found in the metadata.
 */
public class SegmentNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Segment %d not found.";

    /**
     * Creates a new instance of SegmentNotFoundException class.
     *
     * @param segmentNumber missing Segment name
     */
    public SegmentNotFoundException(final int segmentNumber) {
        super(String.format(FORMAT_STRING, segmentNumber));
    }

    /**
     * Creates a new instance of SegmentNotFoundException class.
     *
     * @param segmentNumber missing Segment name
     * @param cause         error cause
     */
    public SegmentNotFoundException(final int segmentNumber, final Throwable cause) {
        super(String.format(FORMAT_STRING, segmentNumber), cause);
    }
}
