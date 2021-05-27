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

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.EventStreamWriterTest.FakeSegmentOutputStream;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class TransactionalEventStreamWriterTest extends ThreadPooledTestSuite {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    private StreamSegments getSegments(Segment segment) {
        NavigableMap<Double, SegmentWithRange> segments = new TreeMap<>();
        segments.put(1.0, new SegmentWithRange(segment, 0.0, 1.0));
        return new StreamSegments(segments);
    }
    
    private CompletableFuture<StreamSegments> getSegmentsFuture(Segment segment) {
        return CompletableFuture.completedFuture(getSegments(segment));
    }
    
    @Test
    public void testTxn() throws TxnFailedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        Transaction<String> txn = writer.beginTxn();
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);
        txn.flush();
        assertTrue(bad.unacked.isEmpty());
        assertTrue(outputStream.unacked.isEmpty());
    }

    @Test
    public void testGetTxn() throws TxnFailedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txnId = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        JavaSerializer<String> serializer = new JavaSerializer<>();

        // Setup mocks
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        Mockito.when(controller.getEpochSegments(scope, streamName, NameUtils.getEpoch(txnId)))
               .thenReturn(getSegmentsFuture(segment));
        Mockito.when(controller.checkTransactionStatus(Stream.of(scope, streamName), txnId))
               .thenReturn(CompletableFuture.completedFuture(Transaction.Status.OPEN));
        Mockito.when(controller.pingTransaction(eq(Stream.of(scope, streamName)), eq(txnId), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(Transaction.PingStatus.OPEN));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txnId)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txnId), any(), any()))
               .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        // Create a transactional event Writer
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        writer.beginTxn();
        writer.close();

        //Create a new transactional eventWriter
        @Cleanup
        TransactionalEventStreamWriter<String> writer1 = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                                                                                                  EventWriterConfig.builder().transactionTimeoutTime(10000).build(),
                                                                                                  executorService());
        Transaction<String> txn = writer1.getTxn(txnId);
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);
        txn.flush();
        assertTrue(bad.unacked.isEmpty());
        assertTrue(outputStream.unacked.isEmpty());
        // verify pings was invoked for the transaction with the specified transactionTimeout.
        verify(controller, timeout(10_000).atLeastOnce()).pingTransaction(eq(stream), eq(txnId), eq(10000L));
    }

    @Test(expected = TxnFailedException.class)
    public void testGetTxnAborted() throws TxnFailedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txnId = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        JavaSerializer<String> serializer = new JavaSerializer<>();

        // Setup mocks
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        Mockito.when(controller.getEpochSegments(scope, streamName, NameUtils.getEpoch(txnId)))
               .thenReturn(getSegmentsFuture(segment));
        Mockito.when(controller.checkTransactionStatus(Stream.of(scope, streamName), txnId))
               .thenReturn(CompletableFuture.completedFuture(Transaction.Status.ABORTED));
        Mockito.when(controller.pingTransaction(eq(Stream.of(scope, streamName)), eq(txnId), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(Transaction.PingStatus.OPEN));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txnId)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txnId), any(), any()))
               .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        // Create a transactional event Writer
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        writer.beginTxn();

        // fetch the txn when it is already aborted
        Transaction<String> txn = writer.getTxn(txnId);
        txn.writeEvent("Foo");
    }

    @Test
    public void testTxnCommit() throws TxnFailedException, SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = spy(new FakeSegmentOutputStream(segment));
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(controller.commitTransaction(eq(stream), anyString(), isNull(), eq(txid))).thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(controller.pingTransaction(eq(stream), eq(txid), anyLong())).thenReturn(CompletableFuture.completedFuture(Transaction.PingStatus.OPEN));
        Mockito.when(controller.checkTransactionStatus(eq(stream), eq(txid))).thenReturn(CompletableFuture.completedFuture(Transaction.Status.OPEN));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
               .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        Transaction<String> txn = writer.beginTxn();
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);
        txn.checkStatus();
        Mockito.verify(controller, Mockito.times(1)).checkTransactionStatus(eq(stream), eq(txid));

        // invoke commit of transaction.
        txn.commit();
        // verify if segments are flushed and closed.
        Mockito.verify(outputStream, Mockito.times(1)).close();
        Mockito.verify(controller, Mockito.times(1)).commitTransaction(eq(stream), anyString(), isNull(), eq(txid));
        assertTrue(bad.unacked.isEmpty());
        assertTrue(outputStream.unacked.isEmpty());
    }

    @Test
    public void testTxnAbort() throws TxnFailedException, SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = spy(new FakeSegmentOutputStream(segment));
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(controller.pingTransaction(eq(stream), eq(txid), anyLong())).thenReturn(CompletableFuture.completedFuture(Transaction.PingStatus.OPEN));
        Mockito.when(controller.abortTransaction(eq(stream), eq(txid))).thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
               .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        Transaction<String> txn = writer.beginTxn();
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);
        // invoke commit of transaction.
        txn.abort();
        // verify if segments are flushed and closed.
        Mockito.verify(outputStream, Mockito.times(1)).close();
        Mockito.verify(controller, Mockito.times(1)).abortTransaction(eq(stream), eq(txid));
    }

    @Test
    public void testTxnFailed() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        Transaction<String> txn = writer.beginTxn();
        outputStream.invokeSealedCallBack();
        try {
            txn.writeEvent("Foo");
        } catch (TxnFailedException e) {
            // Expected
        }
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
    }

    @Test
    public void testTxnCommitFailureRetry() throws TxnFailedException, SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        JavaSerializer<String> serializer = new JavaSerializer<>();

        // Setup mocks
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = spy(new FakeSegmentOutputStream(segment));
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(eq(stream), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(controller.pingTransaction(eq(stream), eq(txid), anyLong())).thenReturn(CompletableFuture.completedFuture(Transaction.PingStatus.OPEN));
        Mockito.when(controller.checkTransactionStatus(eq(stream), eq(txid))).thenReturn(CompletableFuture.completedFuture(Transaction.Status.OPEN));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        // Simulate a Controller client throwing a Deadline Exceeded exception.
        CompletableFuture<Void> failedCommit = new CompletableFuture<>();
        failedCommit.completeExceptionally(new RetriesExhaustedException(new StatusRuntimeException(Status.DEADLINE_EXCEEDED)));
        Mockito.when(controller.commitTransaction(eq(stream), anyString(), isNull(), eq(txid)))
               .thenReturn(failedCommit) // simulate a failure
               .thenReturn(CompletableFuture.completedFuture(null)); // a success.

        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService());
        Transaction<String> txn = writer.beginTxn();
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);

        // invoke commit of transaction.
        try {
            txn.commit();
        } catch (Exception e) {
            assertTrue(e instanceof RetriesExhaustedException && e.getCause() instanceof StatusRuntimeException);
            // the user retries the commit since the TxnFailedException is not thrown.
            txn.commit();
        }

        // verify if segments are flushed and closed.
        Mockito.verify(outputStream, Mockito.times(1)).close();
        Mockito.verify(controller, Mockito.times(2)).commitTransaction(eq(stream), anyString(), isNull(), eq(txid));
        assertTrue(bad.unacked.isEmpty());
        assertTrue(outputStream.unacked.isEmpty());
    }

}
