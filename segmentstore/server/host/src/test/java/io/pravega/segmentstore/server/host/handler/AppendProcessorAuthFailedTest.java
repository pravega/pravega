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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.auth.InvalidTokenException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.InlineExecutor;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AppendProcessorAuthFailedTest {

    private AppendProcessor processor;
    private ServerConnection connection;

    @Before
    public void setUp() throws Exception {
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        connection = mock(ServerConnection.class);
        @Cleanup("shutdown")
        ScheduledExecutorService executor = new InlineExecutor();
        processor = AppendProcessor.defaultBuilder(new IndexAppendProcessor(executor, store))
                                   .store(store)
                                   .connection(new TrackedConnection(connection))
                                   .tokenVerifier((resource, token, expectedLevel) -> {
                                       throw new InvalidTokenException("Token verification failed.");
                                   }).build();
    }

    @Test
    public void setupAppend() {
        processor.setupAppend(new WireCommands.SetupAppend(100L,
                UUID.randomUUID(), "segment", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }
}