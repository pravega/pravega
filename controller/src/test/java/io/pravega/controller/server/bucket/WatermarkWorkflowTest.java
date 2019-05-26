package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.impl.RevisionImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.TestingServerStarter;
import lombok.NonNull;
import lombok.Synchronized;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class WatermarkWorkflowTest {
    TestingServer zkServer;
    CuratorFramework zkClient;

    StreamMetadataStore streamMetadataStore;
    BucketStore bucketStore;
    ScheduledExecutorService executor;
    PeriodicWatermarking periodicWatermarking;
    
    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();

        executor = Executors.newScheduledThreadPool(10);

        streamMetadataStore = StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                AuthHelper.getDisabledAuthHelper(), zkClient, executor);
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 3,
                BucketStore.ServiceType.WatermarkingService, 3);
        bucketStore = StreamStoreFactory.createZKBucketStore(map, zkClient, executor);

        Function<Stream, PeriodicWatermarking.WatermarkClient> supplier = null;
        periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, supplier, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        streamMetadataStore.close();
        ExecutorServiceHelpers.shutdown(executor);

        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }
    
    @Test(timeout = 10000L)
    public void testWatermarkClient() {
        Stream stream = new StreamImpl("scope", "stream");
        SynchronizerClientFactory clientFactory = spy(SynchronizerClientFactory.class);
        
        MockRevisionedStreamClient revisionedClient = new MockRevisionedStreamClient();
        doAnswer(x -> revisionedClient).when(clientFactory).createRevisionedStreamClient(anyString(), any(), any());

        PeriodicWatermarking.WatermarkClient client = new PeriodicWatermarking.WatermarkClient(stream, clientFactory);
        
        // iteration 1
        client.reinitialize();
        // There is no watermark in the stream. All values should be null and all writers active and participating.
        assertNull(revisionedClient.getMark());
        assertTrue(revisionedClient.watermarks.isEmpty());
        assertEquals(client.getPreviousWatermark(), Watermark.EMPTY);
        assertTrue(client.isWriterActive(0L));
        assertTrue(client.isWriterParticipating(0L));
        Watermark first = new Watermark(1L, ImmutableMap.of());
        client.completeIteration(first);

        // iteration 2
        client.reinitialize();
        // There is one watermark. All writers should be active and writers greater than last watermark should be participating 
        assertNull(revisionedClient.getMark());
        assertEquals(revisionedClient.watermarks.size(), 1);
        assertEquals(client.getPreviousWatermark(), first);
        assertTrue(client.isWriterActive(0L));
        assertFalse(client.isWriterParticipating(0L));
        assertTrue(client.isWriterParticipating(1L));
        
        // emit second watermark
        Watermark second = new Watermark(2L, ImmutableMap.of());
        client.completeIteration(second);

        // iteration 3.. do not emit
        client.reinitialize();
        // There are two watermarks. Window size is also 2. So all writers should be active. 
        // All writers after second watermark should be participating. 
        // Mark should be null. 
        assertNull(revisionedClient.getMark());
        assertEquals(2, revisionedClient.watermarks.size());
        assertEquals(client.getPreviousWatermark(), second);
        assertTrue(client.isWriterActive(0L));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));

        // dont emit a watermark but complete this iteration. 
        // This should proceed to shrink the active window.
        client.completeIteration(null);

        // iteration 4.. do not emit
        client.reinitialize();
        // Mark should have been set to first watermark's revision
        // writer's with time before first watermark will be inactive. Writers after second watermark should be participating.  
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(0).getKey());
        assertEquals(revisionedClient.watermarks.size(), 2);
        assertEquals(client.getPreviousWatermark(), second);
        assertFalse(client.isWriterActive(0L));
        assertTrue(client.isWriterActive(1L));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));

        // dont emit a watermark but complete this iteration. This should shrink the window again. 
        client.completeIteration(null);

        // iteration 5
        client.reinitialize();
        // mark should be set to revision of second watermark.
        // active writers should be ahead of second watermark. participating writers should be ahead of second watermark
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 2);
        assertEquals(client.getPreviousWatermark(), second);
        assertFalse(client.isWriterActive(1L));
        assertTrue(client.isWriterActive(2L));
        assertFalse(client.isWriterParticipating(1L));
        assertTrue(client.isWriterParticipating(2L));
        // emit third watermark
        Watermark third = new Watermark(3L, ImmutableMap.of());
        client.completeIteration(third);

        // iteration 6
        client.reinitialize();
        // mark should still be set to revision of second watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 3);
        assertEquals(client.getPreviousWatermark(), third);
        assertFalse(client.isWriterActive(1L));
        assertTrue(client.isWriterActive(2L));
        assertFalse(client.isWriterParticipating(2L));
        assertTrue(client.isWriterParticipating(3L));
        
        // emit fourth watermark
        Watermark fourth = new Watermark(4L, ImmutableMap.of());
        client.completeIteration(fourth);

        // iteration 7
        client.reinitialize();
        // mark should still be set to revision of second watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(1).getKey());
        assertEquals(revisionedClient.watermarks.size(), 4);
        assertEquals(client.getPreviousWatermark(), fourth);
        assertFalse(client.isWriterActive(1L));
        assertTrue(client.isWriterActive(2L));
        assertFalse(client.isWriterParticipating(3L));
        assertTrue(client.isWriterParticipating(4L));

        // emit fifth watermark
        Watermark fifth = new Watermark(5L, ImmutableMap.of());
        client.completeIteration(fifth);

        // iteration 8
        client.reinitialize();
        // mark should progress to third watermark. 
        assertEquals(revisionedClient.getMark(), revisionedClient.watermarks.get(2).getKey());
        assertEquals(revisionedClient.watermarks.size(), 5);
        assertEquals(client.getPreviousWatermark(), fifth);
        assertFalse(client.isWriterActive(2L));
        assertTrue(client.isWriterActive(3L));
        assertFalse(client.isWriterParticipating(4L));
        assertTrue(client.isWriterParticipating(5L));
    }

    @Test
    public void testWatermarkingWorkflow() {
        // 1. create stream
        // 2. note writer1, writer2, writer3 marks
        // 3. run watermarking workflow. When there is no previous watermark for the stream, the workflow will emit a watermark.
        
        // report writer marks for writer1, writer2, writer3 
        // watermark 2 is generated. 
        // now report partial positions. 
    }

    @Test
    public void testComputeWatermarkMethods() {
        // TODO: test various cases cases where watermark is computed. 
        // 1. incomplete positions. 
        // 2. incorporating previous watermarks. 
    }

    class MockRevisionedStreamClient implements RevisionedStreamClient<Watermark> {
        private final AtomicInteger revCounter = new AtomicInteger(0);
        private Revision mark;
        private final List<Map.Entry<Revision, Watermark>> watermarks = new ArrayList<>();
        
        @Override
        @Synchronized
        public Revision fetchOldestRevision() {
            return watermarks.isEmpty() ? null : watermarks.get(0).getKey();
        }

        @Override
        @Synchronized
        public Revision fetchLatestRevision() {
            return watermarks.isEmpty() ? null : watermarks.get(watermarks.size() - 1).getKey();
        }

        @Override
        @Synchronized
        public Iterator<Map.Entry<Revision, Watermark>> readFrom(Revision start) throws TruncatedDataException {
            int index = start == null ? 0 : ((MockRevision) start).id;
            return watermarks.stream().filter(x -> ((MockRevision) x.getKey()).id >= index).iterator();
        }

        @Override
        @Synchronized
        public Revision writeConditionally(Revision latestRevision, Watermark value) {
            Revision last = watermarks.isEmpty() ? null : watermarks.get(watermarks.size() - 1).getKey();
            boolean equal;
            Revision newRevision = null;
            if (latestRevision == null) {
                equal = last == null;
            } else {
                equal = latestRevision.equals(last);
            }

            if (equal) {
                newRevision = new MockRevision(revCounter.incrementAndGet());

                watermarks.add(new AbstractMap.SimpleEntry<>(newRevision, value));
            }
            return newRevision;
        }

        @Override
        @Synchronized
        public void writeUnconditionally(Watermark value) {
            
        }

        @Override
        @Synchronized
        public Revision getMark() {
            return mark;
        }

        @Override
        @Synchronized
        public boolean compareAndSetMark(Revision expected, Revision newLocation) {
            boolean equal;
            if (expected == null) {
                equal = mark == null;
            } else {
                equal = expected.equals(mark);
            }

            if (equal) {
                mark = newLocation;
            }
            return equal;
        }

        @Override
        public void truncateToRevision(Revision revision) {

        }

        @Override
        public void close() {

        }
    }
    
    class MockRevision implements Revision {
        private final int id;

        MockRevision(int id) {
            this.id = id;
        }

        @Override
        public RevisionImpl asImpl() {
            return null;
        }

        @Override
        public int compareTo(@NonNull Revision o) {
            return Integer.compare(id, ((MockRevision)o).id);
        }
    }
}
