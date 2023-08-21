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

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.SecurityConfigDefaults;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class AdminConnectionListenerTest {

    @Test
    public void testCreateEncodingStack() {

        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        AdminConnectionListener listener = new AdminConnectionListener(false, false, "localhost",
                6622, store, mock(TableStore.class), new PassingTokenVerifier(), null, null,
                SecurityConfigDefaults.TLS_PROTOCOL_VERSION, getIndexAppendProcessor(store));
        List<ChannelHandler> stack = listener.createEncodingStack("connection");
        // Check that the order of encoders is the right one.
        Assert.assertTrue(stack.get(0) instanceof ExceptionLoggingHandler);
        Assert.assertTrue(stack.get(1) instanceof CommandEncoder);
        Assert.assertTrue(stack.get(2) instanceof LengthFieldBasedFrameDecoder);
        Assert.assertTrue(stack.get(3) instanceof CommandDecoder);
    }

    @Test
    public void testCreateRequestProcessor() {
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        @Cleanup
        AdminConnectionListener listener = new AdminConnectionListener(false, false, "localhost",
                6622, store, mock(TableStore.class), new PassingTokenVerifier(), null, null,
                SecurityConfigDefaults.TLS_PROTOCOL_VERSION, getIndexAppendProcessor(store));
        Assert.assertTrue(listener.createRequestProcessor(new TrackedConnection(new ServerConnectionInboundHandler())) instanceof AdminRequestProcessorImpl);
    }

    private IndexAppendProcessor getIndexAppendProcessor(StreamSegmentStore store) {
        @Cleanup("shutdown")
        ScheduledExecutorService executor = new InlineExecutor();
        return new IndexAppendProcessor(executor, store);
    }
}
