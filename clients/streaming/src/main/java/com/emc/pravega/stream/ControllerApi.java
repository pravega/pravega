/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.stream;

import com.emc.pravega.controller.stream.api.v1.Status;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Stream Controller APIs.
 */
public final class ControllerApi {

    /**
     * Controller Apis for administrative action for streams
     */
    public interface Admin {
        /**
         * Api to create stream
         * @param streamConfig
         * @return
         */
        CompletableFuture<Status> createStream(StreamConfiguration streamConfig);

        /**
         * Api to alter stream
         * @param streamConfig
         * @return
         */
        CompletableFuture<Status> alterStream(StreamConfiguration streamConfig);
    }

    /**
     * Controller Apis called by pravega producers for getting stream specific information
     */
    public interface Producer {
        /**
         * Api to get list of current segments for the stream to produce to.
         * @param stream
         * @return
         */
        CompletableFuture<StreamSegments> getCurrentSegments(String stream);

        /**
         * Api to get URI for a given segment Id. This will be called when a pravega host fails
         * to respond for the given segment. The producer can check with controller to find new host
         * which is responsible for the said segment.
         * @param segmentId
         * @return
         */
        CompletableFuture<SegmentUri> getURI(String stream, int segmentNumber);
    }

    /**
     * Controller Apis that are called by consumers
     */
    public interface Consumer {
        /**
         * Returns list of position objects by distributing available segments at the
         * given timestamp into requested number of position objects
         * @param stream
         * @param timestamp
         * @param count
         * @return
         */
        CompletableFuture<List<PositionInternal>> getPositions(String stream, long timestamp, int count);

        /**
         * Called by consumer upon reaching end of segment on some segment in its position obejct
         * @param stream
         * @param positions
         * @return
         */
        CompletableFuture<List<PositionInternal>> updatePositions(String stream, List<PositionInternal> positions);

        /**
         * Api to get URI for a given segment Id. This will be called when a pravega host fails
         * to respond for the given segment. The consumer can check with controller to find new host
         * which is responsible for the said segment.
         * @param segmentNumber
         * @return
         */
        CompletableFuture<SegmentUri> getURI(String stream, int segmentNumber);
    }

    //Note: this is not a public interface TODO: Set appropriate scope
    interface Host {
        //Placeholder for APIs that pravega host shall call into
    }
}
