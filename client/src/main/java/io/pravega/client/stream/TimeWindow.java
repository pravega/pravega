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
package io.pravega.client.stream;

import lombok.Data;

/**
 * Represents a time window for the events which are currently being read by a reader.
 * 
 * The lower time bound is a timestamp which is less than or equal to the most recent
 * value provided via the {@link EventStreamWriter#noteTime(long)} API for by any writer using that
 * API at the current location in the stream. If the reader is near the beginning (or the truncation 
 * point) of the stream, there may be no timestamps to compare against and no lower bound can be established. 
 * 
 * Similarly the upper time bound is a timestamp which is greater than or equal to any that were provided by
 * any writer via the {@link EventStreamWriter#noteTime(long)} API prior to the current location in
 * the stream. If a reader is near the end (tail) of the stream there may be no writer timestamps 
 * to compare against and no upper bound can be established.
 *
 * upperTimeBound will always be greater than or equal to lowerTimeBound.
 */
@Data
public class TimeWindow {
    private final Long lowerTimeBound;
    private final Long upperTimeBound;
    
    /**
     * Returns true if the reader is currently near the tail of the stream and therefore no upper time bound can be obtained.
     * @return if a upper bound is unavailable.
     */
    public boolean isNearTailOfStream() {
        return upperTimeBound == null;
    }
    
    /**
     * Returns true if the reader is currently near the head of the stream and therefore no lower time bound can be obtained.
     * @return if a lower bound is unavailable. 
     */
    public boolean isNearHeadOfStream() {
        return lowerTimeBound == null;
    }
}
