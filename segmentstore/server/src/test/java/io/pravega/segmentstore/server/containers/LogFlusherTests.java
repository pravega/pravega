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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.Writer;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.OngoingStubbing;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link LogFlusher} class.
 */
public class LogFlusherTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1;
    private static final long CHECKPOINT_SEQ_NO = 123L;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Tests a case when there is nothing to flush (everything in Storage is up to date).
     */
    @Test
    public void testEmpty() throws Exception {
        val log = mock(OperationLog.class);
        val writer = mock(Writer.class);
        val cleaner = mock(MetadataCleaner.class);
        val flusher = new LogFlusher(CONTAINER_ID, log, writer, cleaner, executorService());
        val order = inOrder(log, writer, cleaner);

        setupOperationLogMock(log, CHECKPOINT_SEQ_NO, 1);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO, 1);
        setupMetadataCleaner(cleaner);
        flusher.flushToStorage(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify that we have a fixed number of invocations in a specific order (since there was nothing to flush).
        verify(order, log, cleaner, writer, CHECKPOINT_SEQ_NO, 1, CHECKPOINT_SEQ_NO, 1);
    }

    /**
     * Tests a case when there is data to flush and everything proceeds without hiccups.
     */
    @Test
    public void testNormalCase() throws Exception {
        val log = mock(OperationLog.class);
        val writer = mock(Writer.class);
        val cleaner = mock(MetadataCleaner.class);
        val flusher = new LogFlusher(CONTAINER_ID, log, writer, cleaner, executorService());
        val order = inOrder(log, writer, cleaner);

        // We set this up so that we have something to flush both before and after the Metadata Persist call; each time,
        // the first Writer invocation reports that it flushed something, and the second time it did not. This should
        // trigger a re-execution of the checkpoint-flush loop.
        setupOperationLogMock(log, CHECKPOINT_SEQ_NO, 4);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO, 2);
        setupMetadataCleaner(cleaner);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO + 2, 2);

        flusher.flushToStorage(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify that we have the correct number of invocations, in the correct order.
        verify(order, log, cleaner, writer, CHECKPOINT_SEQ_NO, 2, 2);
    }

    /**
     * Tests a case when the Writer cannot flush properly (i.e., it never reports that it is fully caught up) before
     * attempting to persist the segment metadata.
     */
    @Test
    public void testNoWriterProgressBefore() {
        val log = mock(OperationLog.class);
        val writer = mock(Writer.class);
        val cleaner = mock(MetadataCleaner.class);
        val flusher = new LogFlusher(CONTAINER_ID, log, writer, cleaner, executorService());
        val order = inOrder(log, writer, cleaner);

        // We set this up so that we have something to flush both before and after the Metadata Persist call; each time,
        // the first Writer invocation reports that it flushed something, and the second time it did not. This should
        // trigger a re-execution of the checkpoint-flush loop.
        setupOperationLogMock(log, CHECKPOINT_SEQ_NO, LogFlusher.MAX_FLUSH_ATTEMPTS);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO, LogFlusher.MAX_FLUSH_ATTEMPTS + 1);
        setupMetadataCleaner(cleaner);

        AssertExtensions.assertSuppliedFutureThrows(
                "Expected the operation to be aborted if no Writer progress could be made.",
                () -> flusher.flushToStorage(TIMEOUT),
                ex -> ex instanceof RetriesExhaustedException);

        // Verify that we have the correct number of invocations, in the correct order.
        verify(order, log, cleaner, writer, CHECKPOINT_SEQ_NO, LogFlusher.MAX_FLUSH_ATTEMPTS, 0);
    }

    /**
     * Tests a case when the Writer cannot flush properly (i.e., it never reports that it is fully caught up) after
     * attempting to persist the segment metadata.
     */
    @Test
    public void testNoWriterProgressAfter() {
        val log = mock(OperationLog.class);
        val writer = mock(Writer.class);
        val cleaner = mock(MetadataCleaner.class);
        val flusher = new LogFlusher(CONTAINER_ID, log, writer, cleaner, executorService());
        val order = inOrder(log, writer, cleaner);

        // We set this up so that we have something to flush both before and after the Metadata Persist call; each time,
        // the first Writer invocation reports that it flushed something, and the second time it did not. This should
        // trigger a re-execution of the checkpoint-flush loop.
        setupOperationLogMock(log, CHECKPOINT_SEQ_NO, LogFlusher.MAX_FLUSH_ATTEMPTS + 2);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO, 2);
        setupMetadataCleaner(cleaner);
        setupWriterMock(writer, CHECKPOINT_SEQ_NO + 2, LogFlusher.MAX_FLUSH_ATTEMPTS + 1);

        AssertExtensions.assertSuppliedFutureThrows(
                "Expected the operation to be aborted if no Writer progress could be made.",
                () -> flusher.flushToStorage(TIMEOUT),
                ex -> ex instanceof RetriesExhaustedException);

        // Verify that we have the correct number of invocations, in the correct order.
        verify(order, log, cleaner, writer, CHECKPOINT_SEQ_NO, 2, LogFlusher.MAX_FLUSH_ATTEMPTS);
    }

    private void verify(InOrder order, OperationLog log, MetadataCleaner cleaner, Writer writer, long baseSeqNo, int preCount, int postCount) {
        verify(order, log, cleaner, writer, baseSeqNo, preCount, baseSeqNo + preCount, postCount);
    }

    private void verify(InOrder order, OperationLog log, MetadataCleaner cleaner, Writer writer, long preBaseSeqNo, int preCount, long postBaseSeqNo, int postCount) {
        for (int i = 0; i < preCount; i++) {
            order.verify(log).checkpoint(any());
            order.verify(writer).forceFlush(eq(preBaseSeqNo + i), any());
        }

        if (postCount > 0) {
            order.verify(cleaner).persistAll(any());
        }

        for (int i = 0; i < postCount; i++) {
            order.verify(log).checkpoint(any());
            order.verify(writer).forceFlush(eq(postBaseSeqNo + i), any());
        }

        order.verifyNoMoreInteractions();
    }

    private void setupMetadataCleaner(MetadataCleaner cleaner) {
        when(cleaner.persistAll(any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void setupOperationLogMock(OperationLog log, long baseSeqNo, int count) {
        OngoingStubbing<CompletableFuture<Long>> m = when(log.checkpoint(any()));
        for (int i = 0; i < count; i++) {
            m = m.thenReturn(CompletableFuture.completedFuture(baseSeqNo + i));
        }
    }

    private void setupWriterMock(Writer writer, long baseSeqNo, int count) {
        for (int i = 0; i < count - 1; i++) {
            when(writer.forceFlush(eq(baseSeqNo + i), any()))
                    .thenReturn(CompletableFuture.completedFuture(true));
        }

        when(writer.forceFlush(eq(baseSeqNo + count - 1), any()))
                .thenReturn(CompletableFuture.completedFuture(false));
    }
}
