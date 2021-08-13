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
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.test.common.AssertExtensions;
import java.util.UUID;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.verify;

public class SegmentTransactionTest {

    @Test(timeout = 5000)
    public void testFlush() throws TxnFailedException, SegmentSealedException {
        UUID uuid = UUID.randomUUID();
        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        @Cleanup
        SegmentTransactionImpl<String> txn = new SegmentTransactionImpl<>(uuid, outputStream, new JavaSerializer<String>());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                PendingEvent event = (PendingEvent) invocation.getArgument(0);
                event.getAckFuture().complete(null);
                return null;
            }
        }).when(outputStream).write(Mockito.any(PendingEvent.class));
        txn.writeEvent("hi");
        verify(outputStream).write(Mockito.any(PendingEvent.class));
        txn.flush();
        verify(outputStream).flush();
        Mockito.verifyNoMoreInteractions(outputStream);
    }

    @Test(timeout = 5000)
    public void testSegmentDoesNotExist() {
        UUID uuid = UUID.randomUUID();
        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        @SuppressWarnings("resource")
        SegmentTransactionImpl<String> txn = new SegmentTransactionImpl<>(uuid, outputStream, new JavaSerializer<String>());
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                PendingEvent event = (PendingEvent) invocation.getArgument(0);
                event.getAckFuture().completeExceptionally(new NoSuchSegmentException("segment"));
                return null;
            }
        }).when(outputStream).write(Mockito.any(PendingEvent.class));
        AssertExtensions.assertThrows(TxnFailedException.class, () -> txn.writeEvent("hi"));
        verify(outputStream).write(Mockito.any(PendingEvent.class));
        AssertExtensions.assertThrows(TxnFailedException.class, () -> txn.flush());
        Mockito.verifyNoMoreInteractions(outputStream);
    }
}
