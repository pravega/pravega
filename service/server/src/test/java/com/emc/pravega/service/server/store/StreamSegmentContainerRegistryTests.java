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

package com.emc.pravega.service.server.store;

import com.emc.nautilus.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ContainerNotFoundException;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.SegmentContainerFactory;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.IntentionalException;
import com.google.common.util.concurrent.AbstractService;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the StreamSegmentContainerRegistry class.
 */
public class StreamSegmentContainerRegistryTests {
    private static final int THREAD_POOL_SIZE = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Tests the getContainer method for registered and unregistered containers.
     */
    @Test
    public void testGetContainer() throws Exception {
        final int containerCount = 1000;
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        TestContainerFactory factory = new TestContainerFactory();
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executor.get());

        HashSet<String> expectedContainerIds = new HashSet<>();
        Collection<CompletableFuture<ContainerHandle>> handleFutures = new ArrayList<>();
        for (int i = 0; i < containerCount; i++) {
            String containerId = getContainerId(i);
            handleFutures.add(registry.startContainer(containerId, TIMEOUT));
            expectedContainerIds.add(containerId);
        }

        Collection<ContainerHandle> handles = FutureHelpers.allOfWithResults(handleFutures).join();
        HashSet<String> actualHandleIds = new HashSet<>();
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
                () -> registry.getContainer("foo"),
                ex -> ex instanceof ContainerNotFoundException);
    }

    /**
     * Tests the ability to stop the container via the stopContainer() method.
     */
    @Test
    public void testStopContainer() throws Exception {
        final String containerId = "Container";
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        TestContainerFactory factory = new TestContainerFactory();
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executor.get());
        ContainerHandle handle = registry.startContainer(containerId, TIMEOUT).join();

        // Register a Listener for the Container.Stop event.
        AtomicReference<String> stopListenerCallback = new AtomicReference<>();
        handle.setContainerStoppedListener(stopListenerCallback::set);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());
        Assert.assertFalse("Container is closed before being shut down.", container.isClosed());

        registry.stopContainer(handle, TIMEOUT).join();
        Thread.sleep(10);
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
        final String containerId = "Container";
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        TestContainerFactory factory = new TestContainerFactory(new IntentionalException());
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executor.get());

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
        final String containerId = "Container";
        @Cleanup
        CloseableExecutorService executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
        TestContainerFactory factory = new TestContainerFactory();
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executor.get());

        ContainerHandle handle = registry.startContainer(containerId, TIMEOUT).join();

        // Register a Listener for the Container.Stop event.
        AtomicReference<String> stopListenerCallback = new AtomicReference<>();
        handle.setContainerStoppedListener(stopListenerCallback::set);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());

        // Fail the container and wait for it to properly terminate.
        container.fail(new IntentionalException());
        ServiceShutdownListener.awaitShutdown(container, false);
        Thread.sleep(20);
        Assert.assertEquals("Unexpected value passed to Handle.stopListenerCallback or callback was not invoked.", containerId, stopListenerCallback.get());
        AssertExtensions.assertThrows(
                "Container is still registered after failure.",
                () -> registry.getContainer(containerId),
                ex -> ex instanceof ContainerNotFoundException);
    }

    private String getContainerId(int i) {
        return "Container_" + i;
    }

    //region TestContainerFactory

    private static class TestContainerFactory implements SegmentContainerFactory {
        private final Exception startException;

        public TestContainerFactory() {
            this(null);
        }

        public TestContainerFactory(Exception startException) {
            this.startException = startException;
        }

        @Override
        public SegmentContainer createStreamSegmentContainer(String containerId) {
            return new TestContainer(containerId, this.startException);
        }
    }

    //endregion

    //region TestContainer

    private static class TestContainer extends AbstractService implements SegmentContainer {
        private final String id;
        private Exception startException;
        private Exception stopException;
        private boolean closed;

        public TestContainer(String id, Exception startException) {
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
        public String getId() {
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
        public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<String> createBatch(String parentStreamSegmentName, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Long> mergeBatch(String batchName, Duration timeout) {
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
        public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId, Duration timeout) {
            return null;
        }

        //endregion
    }

    //endregion
}
