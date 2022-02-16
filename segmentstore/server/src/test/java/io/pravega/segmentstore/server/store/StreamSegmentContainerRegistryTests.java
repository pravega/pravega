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
package io.pravega.segmentstore.server.store;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.server.DebugSegmentContainer;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.ContainerHandle;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.ServiceListeners;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the StreamSegmentContainerRegistry class.
 */
public class StreamSegmentContainerRegistryTests extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

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

        List<ContainerHandle> handles = Futures.allOfWithResults(handleFutures).join();
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
     * Tests the getContainers method for registered and unregistered containers.
     */
    @Test
    public void testGetContainers() throws Exception {
        final int containerCount = 1000;
        TestContainerFactory factory = new TestContainerFactory();
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        HashSet<Integer> expectedContainerIds = new HashSet<>();
        for (int containerId = 0; containerId < containerCount; containerId++) {
            registry.startContainer(containerId, TIMEOUT);
            expectedContainerIds.add(containerId);
        }

        HashSet<Integer> actualHandleIds = new HashSet<>();
        for (SegmentContainer segmentContainer : registry.getContainers()) {
            actualHandleIds.add(segmentContainer.getId());
            Assert.assertTrue("Wrong container Java type.", segmentContainer instanceof TestContainer);
            segmentContainer.close();
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

        // Register a Listener for the Container.Stop event. Make this a Future since these callbacks are invoked async
        // so they may finish executing after stop() finished.
        CompletableFuture<Integer> stopListenerCallback = new CompletableFuture<>();
        handle.setContainerStoppedListener(stopListenerCallback::complete);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());
        Assert.assertFalse("Container is closed before being shut down.", container.isClosed());

        registry.stopContainer(handle, TIMEOUT).join();
        Assert.assertEquals("Unexpected value passed to Handle.stopListenerCallback or callback was not invoked.",
                containerId, (int) stopListenerCallback.join());
        Assert.assertTrue("Container is not closed after being shut down.", container.isClosed());
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

        // We insert a ReusableLatch that will allow us to manually delay the TestContainer's shutdown/closing process
        // so that we have enough time to verify that calling getContainer() on a currently shutting down container will
        // throw the appropriate exception.
        ReusableLatch closeReleaseSignal = new ReusableLatch();
        TestContainerFactory factory = new TestContainerFactory(new IntentionalException(), closeReleaseSignal);
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        AssertExtensions.assertThrows(
                "Unexpected exception thrown upon failed container startup.",
                registry.startContainer(containerId, TIMEOUT)::join,
                ex -> ex instanceof IntentionalException || (ex instanceof IllegalStateException && ex.getCause() instanceof IntentionalException));

        AssertExtensions.assertThrows(
                "Container is registered even if it failed to start (and is currently shut down).",
                () -> registry.getContainer(containerId),
                ex -> ex instanceof ContainerNotFoundException);

        // Unblock container closing, which will, in turn, unblock its de-registration.
        closeReleaseSignal.release();

        AssertExtensions.assertThrows(
                "Container is registered even if it failed to start (and has been unregistered).",
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

        // Register a Listener for the Container.Stop event. Make this a Future since these callbacks are invoked async
        // so they may finish executing after stop() finished.
        CompletableFuture<Integer> stopListenerCallback = new CompletableFuture<>();
        handle.setContainerStoppedListener(stopListenerCallback::complete);

        TestContainer container = (TestContainer) registry.getContainer(handle.getContainerId());

        // Fail the container and wait for it to properly terminate.
        container.fail(new IntentionalException());
        ServiceListeners.awaitShutdown(container, false);
        Assert.assertEquals("Unexpected value passed to Handle.stopListenerCallback or callback was not invoked.",
                containerId, (int) stopListenerCallback.join());
        AssertExtensions.assertThrows(
                "Container is still registered after failure.",
                () -> registry.getContainer(containerId),
                ex -> ex instanceof ContainerNotFoundException);
    }

    /**
     * Tests a scenario where a container startup is requested immediately after the shutdown of the same container or
     * while that one is running. This tests both the case when a container auto-shuts down due to failure and when it
     * is shut down in a controlled manner.
     */
    @Test
    public void testStartAlreadyRunning() throws Exception {
        final int containerId = 1;
        TestContainerFactory factory = new TestContainerFactory();
        @Cleanup
        StreamSegmentContainerRegistry registry = new StreamSegmentContainerRegistry(factory, executorService());

        registry.startContainer(containerId, TIMEOUT).join();
        TestContainer container1 = (TestContainer) registry.getContainer(containerId);

        // 1. While running.
        AssertExtensions.assertThrows("startContainer() did not throw for already registered container.",
                () -> registry.startContainer(containerId, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // 2. After a container fails - while shutting down.
        container1.stopSignal = new ReusableLatch(); // Manually control when the Container actually shuts down.
        container1.fail(new IntentionalException());
        val startContainer2 = registry.startContainer(containerId, TIMEOUT);
        Assert.assertFalse("startContainer() completed before previous container shut down (with failure).", startContainer2.isDone());

        container1.stopSignal.release();
        startContainer2.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        TestContainer container2 = (TestContainer) registry.getContainer(containerId);

        Assert.assertEquals("Container1 was not shut down (with failure).", Service.State.FAILED, container1.state());
        Assert.assertEquals("Container2 was not started properly.", Service.State.RUNNING, container2.state());

        // 3. After a controlled shutdown - while shutting down.
        container2.stopSignal = new ReusableLatch(); // Manually control when the Container actually shuts down.
        container2.stopAsync();
        val startContainer3 = registry.startContainer(containerId, TIMEOUT);
        Assert.assertFalse("startContainer() completed before previous container shut down (normally).", startContainer3.isDone());

        container2.stopSignal.release();
        startContainer3.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        TestContainer container3 = (TestContainer) registry.getContainer(containerId);

        Assert.assertEquals("Container2 was not shut down (normally).", Service.State.TERMINATED, container2.state());
        Assert.assertEquals("Container3 was not started properly.", Service.State.RUNNING, container3.state());
    }

    //region TestContainerFactory

    private class TestContainerFactory implements SegmentContainerFactory {
        private final Exception startException;
        private final ReusableLatch startReleaseSignal;

        TestContainerFactory() {
            this(null, null);
        }

        TestContainerFactory(Exception startException, ReusableLatch startReleaseSignal) {
            this.startException = startException;
            this.startReleaseSignal = startReleaseSignal;
        }

        @Override
        public DebugSegmentContainer createDebugStreamSegmentContainer(int containerId) {
            throw new UnsupportedOperationException("DebugSegmentContainer not supported in container Registry Tests.");
        }

        @Override
        public SegmentContainer createStreamSegmentContainer(int containerId) {
            return new TestContainer(containerId, this.startException, this.startReleaseSignal);
        }
    }

    //endregion

    //region TestContainer

    private class TestContainer extends AbstractService implements SegmentContainer {
        private final int id;
        private final Exception startException;
        private final ReusableLatch closeReleaseSignal;
        private Exception stopException;
        private final AtomicBoolean closed;
        private ReusableLatch stopSignal;

        TestContainer(int id, Exception startException, ReusableLatch closeReleaseSignal) {
            this.id = id;
            this.startException = startException;
            this.closeReleaseSignal = closeReleaseSignal;
            this.closed = new AtomicBoolean();
        }

        public void fail(Exception ex) {
            this.stopException = ex;
            stopAsync();
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        @Override
        public int getId() {
            return this.id;
        }

        @Override
        public boolean isOffline() {
            return false;
        }

        @Override
        public void close() {
            if (!this.closed.getAndSet(true)) {
                Futures.await(Services.stopAsync(this, executorService()));
                ReusableLatch signal = this.closeReleaseSignal;
                if (signal != null) {
                    // Wait until we are told to complete.
                    signal.awaitUninterruptibly();
                }
            }
        }

        @Override
        protected void doStart() {
            executorService().execute(() -> {
                if (this.startException != null) {
                    notifyFailed(this.startException);
                } else {
                    notifyStarted();
                }
            });
        }

        @Override
        protected void doStop() {
            executorService().execute(() -> {
                ReusableLatch signal = this.stopSignal;
                if (signal != null) {
                    // Wait until we are told to stop.
                    signal.awaitUninterruptibly();
                }

                if (state() != State.FAILED && state() != State.TERMINATED && this.stopException != null) {
                    notifyFailed(this.stopException);
                } else {
                    notifyStopped();
                }
            });
        }

        //region Unimplemented methods

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
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
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType, Collection<AttributeUpdate> attributes, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment,
                                                                              AttributeUpdateCollection attributes, Duration timeout) {
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
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> forSegment(String streamSegmentName, OperationPriority priority, Duration timeout) {
            return null;
        }

        @Override
        public Collection<SegmentProperties> getActiveSegments() {
            return null;
        }

        @Override
        public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
            return null;
        }

        @Override
        public CompletableFuture<Void> flushToStorage(Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
            return null;
        }

        //endregion
    }

    //endregion
}
