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
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.EventExecutor;
import io.pravega.shared.metrics.ClientMetricKeys;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.AtMost;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CommandEncoderTest {

    @RequiredArgsConstructor
    private static class TestBatchSizeTracker implements AppendBatchSizeTracker {
        private final int size;

        @Override
        public void recordAppend(long eventNumber, int size) {
        }

        @Override
        public long recordAck(long eventNumber) {
            return 0;
        }

        @Override
        public int getAppendBlockSize() {
            return size;
        }

        @Override
        public int getBatchTimeout() {
            return Integer.MAX_VALUE;
        }
    }

    @Test
    public void testFlushing() throws Exception {
        UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(false, false);
        CommandEncoder commandEncoder = new CommandEncoder(s -> new TestBatchSizeTracker(0), new TestMetricNotifier());
        verifyFlush(commandEncoder, allocator, new Hello(1, 2));
        verifyFlush(commandEncoder, allocator, new KeepAlive());
        UUID uuid = new UUID(1, 2);
        verifyFlush(commandEncoder, allocator, new SetupAppend(1, uuid, "segment", ""));
        verifyFlush(commandEncoder, allocator, new Append("segment", uuid, 1L, new Event(allocator.buffer()), 1L));

        allocator = new UnpooledByteBufAllocator(false, false);
        commandEncoder = new CommandEncoder(s -> new TestBatchSizeTracker(1000), new TestMetricNotifier());
        verifyFlush(commandEncoder, allocator, new Hello(1, 2));
        verifyFlush(commandEncoder, allocator, new KeepAlive());
        verifyFlush(commandEncoder, allocator, new SetupAppend(1, uuid, "segment", ""));
        ByteBuf buffer = allocator.buffer();
        verifyNoFlush(commandEncoder, allocator, new Append("segment", uuid, 1L, new Event(buffer), 1L));
        buffer = allocator.buffer();
        buffer.writeBytes(new byte[400]);
        verifyNoFlush(commandEncoder, allocator, new Append("segment", uuid, 2L, new Event(buffer), 1L));
        buffer = allocator.buffer();
        buffer.writeBytes(new byte[400]);
        verifyNoFlush(commandEncoder, allocator, new Append("segment", uuid, 3L, new Event(buffer), 1L));
        buffer = allocator.buffer();
        buffer.writeBytes(new byte[400]);
        verifyFlush(commandEncoder, allocator, new Append("segment", uuid, 4L, new Event(buffer), 1L));
        buffer = allocator.buffer();
        buffer.writeBytes(new byte[400]);
        verifyNoFlush(commandEncoder, allocator, new Append("segment", uuid, 5L, new Event(buffer), 1L));
        verifyFlush(commandEncoder, allocator, new ReadSegment("segment", 0, 1000, "", 2L));
    }

    public void verifyFlush(CommandEncoder commandEncoder, UnpooledByteBufAllocator allocator,
                            Object command) throws Exception {
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        Mockito.when(context.alloc()).thenReturn(allocator);
        Mockito.when(context.executor()).thenReturn(Mockito.mock(EventExecutor.class));
        commandEncoder.write(context, command, null);
        verify(context).alloc();
        verify(context).write(Mockito.any(), Mockito.any());
        verify(context).flush();
        verify(context, new AtMost(10)).executor(); // Irrelevant
        verify(context, new AtMost(10)).channel(); // Irrelevant
        verifyNoMoreInteractions(context);
    }

    public void verifyNoFlush(CommandEncoder commandEncoder, UnpooledByteBufAllocator allocator,
                              Object command) throws Exception {
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        Mockito.when(context.alloc()).thenReturn(allocator);
        Mockito.when(context.executor()).thenReturn(Mockito.mock(EventExecutor.class));
        commandEncoder.write(context, command, null);
        verify(context).alloc();
        verify(context).write(Mockito.any(), Mockito.any());
        verify(context, new AtMost(10)).executor(); // Irrelevant
        verify(context, new AtMost(10)).channel(); // Irrelevant
        verifyNoMoreInteractions(context);
    }

    /**
     * Added a mock MetricNotifier different from the default one to exercise reporting metrics from client side.
     */
    static class TestMetricNotifier implements MetricNotifier {
        @Override
        public void updateSuccessMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
            NO_OP_METRIC_NOTIFIER.updateSuccessMetric(metricKey, metricTags, value);
            assertNotNull(metricKey);
        }

        @Override
        public void updateFailureMetric(ClientMetricKeys metricKey, String[] metricTags, long value) {
            NO_OP_METRIC_NOTIFIER.updateFailureMetric(metricKey, metricTags, value);
            assertNotNull(metricKey);
        }

        @Override
        public void close() {
            NO_OP_METRIC_NOTIFIER.close();
        }
    }

}
