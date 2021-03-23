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
package io.pravega.segmentstore.server;

import java.util.concurrent.CompletableFuture;

/**
 * Defines debug segment container for stream segments.
 */
public interface DebugSegmentContainer extends SegmentContainer {

    /**
     * Updates container metadata table with the given details of a segment. This is used during the data recovery
     * process when a segment exists in the long term storage, but not in the durable data log.
     * @param streamSegmentName         Name of the segment to be registered.
     * @param length                    Length of the segment to be registered.
     * @param isSealed                  Sealed status of the segment to be registered.
     * @return                          A CompletableFuture that, when completed normally, will indicate the operation
     * completed. If the operation failed, the future will be failed with the causing exception.
     */
    CompletableFuture<Void> registerSegment(String streamSegmentName, long length, boolean isSealed);
}
