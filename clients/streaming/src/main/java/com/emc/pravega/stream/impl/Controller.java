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

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;


/**
 * Stream Controller APIs.
 */
public interface Controller {

    // Controller Apis for administrative action for streams
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
    
     // Controller Apis called by pravega producers for getting stream specific information
     
        /**
         * Api to get list of current segments for the stream to produce to.
         */
        CompletableFuture<StreamSegments> getCurrentSegments(String scope, String streamName);
        
        void createTransaction(Stream stream, UUID txId, long timeout);

        void commitTransaction(Stream stream, UUID txId) throws TxFailedException;

        void dropTransaction(Stream stream, UUID txId);

        Transaction.Status checkTransactionStatus(UUID txId);

    // Controller Apis that are called by consumers

        /**
         * Returns list of position objects by distributing available segments at the
         * given timestamp into requested number of position objects
         * @param stream
         * @param timestamp
         * @param count
         * @return
         */
        CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count);

        /**
         * Called by consumer upon reaching end of segment on some segment in its position obejct
         * @param stream
         * @param positions
         * @return
         */
        CompletableFuture<List<PositionInternal>> updatePositions(Stream stream, List<PositionInternal> positions);

    //Controller Apis that are called by producers and consumers
        
        /**
         * Given a segment return the endpoint that currently is the owner of that segment.
         * 
         * The result of this function can be cached until the endpoint is unreachable or indicates it
         * is no longer the owner.
         * 
         * @param qualifiedSegmentName The name of the segment. Usually obtained from {@link Segment#getQualifiedName()}.
         */
        CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName);
   
}
