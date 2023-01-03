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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReaderTests;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ReadOnlySegmentContainer class.
 */
public class ReadOnlySegmentContainerTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_LENGTH = 3 * ReadOnlySegmentContainer.MAX_READ_AT_ONCE_BYTES;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SEGMENT_NAME = "Segment";
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the getStreamSegmentInfo() method.
     */
    @Test
    public void testGetStreamSegmentInfo() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();

        // Non-existent segment.
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected exception when the segment does not exist.",
                () -> context.container.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Create a segment, add some data, set some attributes, "truncate" it and then seal it.
        val storageInfo = context.storage.create(SEGMENT_NAME, TIMEOUT)
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT))
                .thenCompose(v -> context.storage.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT)).join();
        val expectedInfo = StreamSegmentInformation.from(storageInfo)
                .startOffset(storageInfo.getLength() / 2)
                .attributes(ImmutableMap.of(AttributeId.randomUUID(), 100L, Attributes.EVENT_COUNT, 1L))
                .build();

        // Fetch the SegmentInfo from the ReadOnlyContainer and verify it is as expected.
        val actual = context.container.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertEquals("Unexpected Name.", expectedInfo.getName(), actual.getName());
        Assert.assertEquals("Unexpected Length.", expectedInfo.getLength(), actual.getLength());
        Assert.assertEquals("Unexpected Sealed status.", expectedInfo.isSealed(), actual.isSealed());
    }

    /**
     * Tests the read() method.
     */
    @Test
    public void testRead() throws Exception {
        final int truncationOffset = SEGMENT_LENGTH / 2;
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();
        val writtenData = populate(SEGMENT_LENGTH, truncationOffset, context);
        val segmentInfo = context.container.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT).join();
        @Cleanup
        val rr = context.container.read(SEGMENT_NAME, 0, writtenData.length, TIMEOUT).join();
        StreamSegmentStorageReaderTests.verifyReadResult(rr, segmentInfo, 0, writtenData.length, writtenData);
    }

    /**
     * Tests the read() method when the segment does not exist.
     */
    @Test
    public void testReadInexistentSegment() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();

        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected exception when the segment does not exist.",
                () -> context.container.read(SEGMENT_NAME, 0, 1, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Verifies that non-read operations are not supported.
     */
    @Test
    public void testUnsupportedOperations() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();
        assertUnsupported("createStreamSegment", () -> context.container.createStreamSegment(SEGMENT_NAME, SegmentType.STREAM_SEGMENT, null, TIMEOUT));
        assertUnsupported("append", () -> context.container.append(SEGMENT_NAME, new ByteArraySegment(new byte[1]), null, TIMEOUT));
        assertUnsupported("append-offset", () -> context.container.append(SEGMENT_NAME, 0, new ByteArraySegment(new byte[1]), null, TIMEOUT));
        assertUnsupported("sealStreamSegment", () -> context.container.sealStreamSegment(SEGMENT_NAME, TIMEOUT));
        assertUnsupported("truncateStreamSegment", () -> context.container.truncateStreamSegment(SEGMENT_NAME, 0, TIMEOUT));
        assertUnsupported("deleteStreamSegment", () -> context.container.deleteStreamSegment(SEGMENT_NAME, TIMEOUT));
        assertUnsupported("mergeTransaction", () -> context.container.mergeStreamSegment(SEGMENT_NAME, SEGMENT_NAME, TIMEOUT));
        assertUnsupported("mergeTransaction", () -> context.container.mergeStreamSegment(SEGMENT_NAME, SEGMENT_NAME, new AttributeUpdateCollection(), TIMEOUT));
        assertUnsupported("getExtendedChunkInfo", () -> context.container.getExtendedChunkInfo(SEGMENT_NAME, TIMEOUT));
    }

    private byte[] populate(int length, int truncationOffset, TestContext context) {
        val rnd = new Random(0);
        byte[] data = new byte[length];
        rnd.nextBytes(data);

        context.storage.create(SEGMENT_NAME, TIMEOUT)
                       .thenCompose(handle -> context.storage
                               .write(handle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT)
                               .thenCompose(v -> context.storage.truncate(handle, truncationOffset, TIMEOUT))).join();
        return data;
    }

    private void assertUnsupported(String name, Supplier<CompletableFuture<?>> toTest) {
        AssertExtensions.assertSuppliedFutureThrows(
                name + "did not throw expected exception.",
                toTest::get,
                ex -> ex instanceof UnsupportedOperationException);
    }

    private class TestContext implements AutoCloseable {
        final SegmentContainer container;
        final Storage storage;
        private final InMemoryStorageFactory storageFactory;

        TestContext() {
            this.storageFactory = new InMemoryStorageFactory(executorService());
            this.container = new ReadOnlySegmentContainer(this.storageFactory, executorService());
            this.storage = this.storageFactory.createStorageAdapter();
        }

        @Override
        public void close() {
            this.container.close();
            this.storage.close();
            this.storageFactory.close();
        }
    }
}
