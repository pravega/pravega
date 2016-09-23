/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.stream.ConnectionClosedException;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

public class SegmentHelper {

    public static boolean createSegment(String name, SegmentUri uri) {
        ConnectionFactory clientCF = new ConnectionFactoryImpl(false);

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
        ClientConnection connection = getAndHandleExceptions(clientCF.establishConnection(uri.getEndpoint(), uri.getPort(), replyProcessor),
                RuntimeException::new);
        try {
            connection.send(new WireCommands.CreateSegment(name));
        } catch (ConnectionFailedException e) {
            throw new RuntimeException(e);
        }
        return getAndHandleExceptions(result, RuntimeException::new);
    }
}
