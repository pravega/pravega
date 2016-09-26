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

package com.emc.pravega.stream.impl;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.netty.SegmentUri;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.Transaction;

import ch.qos.logback.core.status.Status;

/**
 * Stream Controller APIs.
 */
public final class Controller {

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

        void commitTransaction(Stream stream, UUID txId);

        void dropTransaction(Stream stream, UUID txId);

        Transaction.Status checkTransactionStatus(UUID txId);

        void createTransaction(SegmentId s, UUID txId, long timeout);

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
    }

    public interface Host {
        /**
         * Given a segment return the endpoint that currently is the owner of that segment.
         * 
         * The result of this function can be cached until the endpoint is unreachable or indicates it
         * is no longer the owner.
         * 
         * @param qualifiedSegmentName The name of the segment. Usually obtained from {@link SegmentId#getQualifiedName()}.
         */
        SegmentUri getEndpointForSegment(String qualifiedSegmentName);
    }
}
