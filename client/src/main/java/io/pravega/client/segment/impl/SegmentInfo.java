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

import com.google.common.annotations.Beta;
import lombok.Data;

/**
 * Information about a segment of a stream.
 */
@Beta
@Data
public class SegmentInfo {

    /**
     * Which segment these properties relate to.
     */
    private final Segment segment;
    
    /**
     * The offset at which data is available. In the event the stream has never been truncated this
     * is 0. However, if all data below a certain offset has been truncated, that offset will be
     * provide here. (Offsets are left absolute even if data is truncated so that positions in the
     * segment can be referred to consistently)
     */
    private final long startingOffset;
    
    /**
     * The offset at which new data would be written if it were to be added. This is equal to the
     * total length of all data written to the segment.
     */
    private final long writeOffset;
    
    /**
     * If the segment is sealed and can no longer be written to.
     */
    private final boolean isSealed;
    
    /**
     * The last time the segment was written to.
     */
    private final long lastModifiedTime;

}
