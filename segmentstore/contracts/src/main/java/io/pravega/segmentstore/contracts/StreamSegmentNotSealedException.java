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

public class StreamSegmentNotSealedException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentNotSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is not sealed.
     */
    public StreamSegmentNotSealedException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment is not sealed.", null);
    }
}