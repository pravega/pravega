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
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.stream.Segment;

/**
 * Creates {@link SegmentInputStream} for reading from existing segments.
 */
public interface SegmentInputStreamFactory {
    /**
     * Opens an existing segment for reading. This operation will fail if the
     * segment does not exist.
     * This operation may be called multiple times on the same stream from the
     * same client (i.e., there can be concurrent Stream Readers in the same
     * process space).
     *
     * @param segment The segment to create an input for.
     * @param config  The SegmentInputConfiguration to use.
     */
    SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config);
}