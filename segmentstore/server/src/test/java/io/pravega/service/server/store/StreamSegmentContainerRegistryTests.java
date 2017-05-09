/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.store;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.concurrent.ServiceShutdownListener;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.ContainerNotFoundException;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.server.ContainerHandle;
import io.pravega.service.server.SegmentContainer;
import io.pravega.service.server.SegmentContainerFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import com.google.common.util.concurrent.AbstractService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the StreamSegmentContainerRegistry class.
 */
public class StreamSegmentContainerRegistryTests extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Tests the getContainer method for registered and unregistered containers.
     */
    @Test
    public void testGetContainer() throws Exception {
        final int containerCount = 1000;
        TestContainerFactory factory = new TestContainerFactory();
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        HashSet<Integer> expectedContainerIds = new HashSet<>();
        List<CompletableFuture<ContainerHandle>> handleFutures = new ArrayList<>();
        for (int containerId = 0; containerId < containerCount; containerId++) {
            handleFutures.add(registry.startContainer(containerId, TIMEOUT));
            expectedContainerIds.add(containerId);
        }

        List<ContainerHandle> handles = FutureHelpers.allOfWithResults(handleFutures).join();
        HashSet<Integer> actualHandleIds = new HashSet<>();
        for (ContainerHandle handle : handles) {
            actualHandleIds.add(handle.getContainerId());
            SegmentContainer container = registry.getContainer(handle.getContainerId());
            Assert.assertTrue("Wrong container Java type.", container instanceof TestContainer);
            Assert.assertEquals("Unexpected container Id.", handle.getContainerId(), container.getId());
            container.close();
        }

        AssertExtensions.assertContainsSameElements("Unexpected container ids registered.", expectedContainerIds, actualHandleIds);

        AssertExtensions.assertThrows(
                "getContainer did not throw when passed an invalid container id.",
                () -> registry.getContainer(containerCount + 1),
                ex -> ex instanceof ContainerNotFoundException);
    }

    /**
     * Tests the ability to stop the container via the stopContainer() method.
     */
    @Test
    public void testStopContainer() throws Exception {
        final int containerId = 123;
        TestContainerFactory factory = new TestContainerFactory();
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());
        ContainerHandle handle = registry.startContainer(containerId, TIMEOUT).join();

        // Register a Listener for the Container.Stop event.
        AtomicInteger stopListenerCallback = new AtomicInteger();
        handle.setContainerStoppedListener(stopListenerCallback::set);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());
        Assert.assertFalse("Container is closed before being shut down.", container.isClosed());

        registry.stopContainer(handle, TIMEOUT).join();
        Assert.assertTrue("Container is not closed after being shut down.", container.isClosed());
        Assert.assertEquals("Unexpected value passed to Handle.stopListenerCallback or callback was not invoked.", containerId, stopListenerCallback.get());
        AssertExtensions.assertThrows(
                "Container is still registered after being shut down.",
                () -> registry.getContainer(handle.getContainerId()),
                ex -> ex instanceof ContainerNotFoundException);
    }

    /**
     * Tests the ability to detect a container failure and unregister the container in case the container fails on startup.
     */
    @Test
    public void testContainerFailureOnStartup() throws Exception {
        final int containerId = 123;
        TestContainerFactory factory = new TestContainerFactory(new IntentionalException());
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        AssertExtensions.assertThrows(
                "Unexpected exception thrown upon failed container startup.",
                registry.startContainer(containerId, TIMEOUT)::join,
                ex -> ex instanceof IntentionalException || (ex instanceof IllegalStateException && ex.getCause() instanceof IntentionalException));

        AssertExtensions.assertThrows(
                "Container is registered even if it failed to start.",
                () -> registry.getContainer(containerId),
                ex -> ex instanceof ContainerNotFoundException);
    }

    /**
     * Tests the ability to detect a container failure and unregister the container in case the container fails while running.
     */
    @Test
    public void testContainerFailureWhileRunning() throws Exception {
        final int containerId = 123;
        TestContainerFactory factory = new TestContainerFactory();
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        ContainerHandle handle = registry.startContainer(containerId, TIMEOUT).join();

        // Register a Listener for the Container.Stop event.
        AtomicInteger stopListenerCallback = new AtomicInteger();
        handle.setContainerStoppedListener(stopListenerCallback::set);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());

        // Fail the container and wait for it to properly terminate.
        container.fail(new IntentionalException());
        ServiceShutdownListener.awaitShutdown(container, false);
        Assert.assertEquals("Unexpected value passed to Handle.stopListenerCallback or callback was not invoked.", containerId, stopListenerCallback.get());
        AssertExtensions.assertThrows(
                "Container is still registered after failure.",
                () -> registry.getContainer(containerId),
                ex -> ex instanceof ContainerNotFoundException);
    }

    //region TestContainerFactory

    private static class TestContainerFactory implements SegmentContainerFactory {
        private final Exception startException;

        TestContainerFactory() {
            this(null);
        }

        TestContainerFactory(Exception startException) {
            this.startException = startException;
        }

        @Override
        public SegmentContainer createStreamSegmentContainer(int containerId) {
            return new TestContainer(containerId, this.startException);
        }
    }

    //endregion

    //region TestContainer

    private static class TestContainer extends AbstractService implements SegmentContainer {
        private final int id;
        private final Exception startException;
        private Exception stopException;
        private boolean closed;

        TestContainer(int id, Exception startException) {
            this.id = id;
            this.startException = startException;
        }

        public void fail(Exception ex) {
            this.stopException = ex;
            stopAsync();
        }

        public boolean isClosed() {
            return this.closed;
        }

        @Override
        public int getId() {
            return this.id;
        }

        @Override
        public void close() {
            if (!this.closed) {
                stopAsync();
                ServiceShutdownListener.awaitShutdown(this, false);
                this.closed = true;
            }
        }

        @Override
        protected void doStart() {
            if (this.startException != null) {
                notifyFailed(this.startException);
            } else {
                notifyStarted();
            }
        }

        @Override
        protected void doStop() {
            if (state() != State.FAILED && state() != State.TERMINATED && this.stopException != null) {
                notifyFailed(this.stopException);
            } else {
                notifyStopped();
            }
        }

        //region Unimplemented methods

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId, Collection<AttributeUpdate> attributes, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
            return null;
        }

        @Override
        public Collection<SegmentProperties> getActiveSegments() {
            return null;
        }

        //endregion
    }

    //endregion
}
