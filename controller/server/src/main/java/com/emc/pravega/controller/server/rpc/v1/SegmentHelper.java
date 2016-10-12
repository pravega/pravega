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

package com.emc.pravega.controller.server.rpc.v1;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.hash.HashHelper;
import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.Segment;

public class SegmentHelper {

    public static NodeUri getSegmentUri(String scope, String stream, int segmentNumber, HostControllerStore hostStore) {
        int container = HashHelper.seededWith("SegmentHelper").hashToBucket(stream + segmentNumber, hostStore.getContainerCount());
        Host host = hostStore.getHostForContainer(container);
        return new NodeUri(host.getIpAddr(), host.getPort());
    }
    
    public static boolean createSegment(String scope, String stream, int segmentNumber, PravegaNodeUri uri, ConnectionFactory clientCF) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };
        ClientConnection connection = FutureHelpers.getAndHandleExceptions(clientCF.establishConnection(uri, replyProcessor),
                RuntimeException::new);
        try {
            connection.send(new WireCommands.CreateSegment(Segment.getQualifiedName(scope, stream, segmentNumber)));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return FutureHelpers.getAndHandleExceptions(result, RuntimeException::new);
    }

    public static boolean sealSegment(String scope, String stream, int segmentNumber, PravegaNodeUri uri, ConnectionFactory clientCF) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnknownHostException());
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };
        ClientConnection connection = FutureHelpers.getAndHandleExceptions(clientCF.establishConnection(uri, replyProcessor),
                RuntimeException::new);
        try {

            connection.send(new WireCommands.SealSegment(Segment.getQualifiedName(scope, stream, segmentNumber)));

        } catch (ConnectionFailedException e) {

            throw new RuntimeException(e);

        }
        return FutureHelpers.getAndHandleExceptions(result, RuntimeException::new);
    }

}
