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
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class BucketStoreTest {
    BucketStore bucketStore;
    ScheduledExecutorService executorService;

    abstract BucketStore getBucketStore(ImmutableMap<BucketStore.ServiceType, Integer> map);
    
    @Before
    public void setUp() throws Exception {
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

        ImmutableMap<BucketStore.ServiceType, Integer> map =
                ImmutableMap.of(BucketStore.ServiceType.RetentionService, 2, BucketStore.ServiceType.WatermarkingService, 3);
        bucketStore = getBucketStore(map);
    }
    
    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executorService);
    }
    
    @Test(timeout = 10000L)
    public void testBucketStore() {
        testBucketStore(BucketStore.ServiceType.RetentionService);
        testBucketStore(BucketStore.ServiceType.WatermarkingService);
    }

    private void testBucketStore(BucketStore.ServiceType serviceType) {
        String scope = "scope";
        String stream1 = "stream1";

        int bucketCount = bucketStore.getBucketCount(serviceType);
        
        bucketStore.addStreamToBucketStore(serviceType, scope, stream1, executorService).join();
        int bucket = BucketStore.getBucket(scope, stream1, bucketCount);
        Set<String> streamsInBucket = bucketStore.getStreamsForBucket(serviceType, bucket, executorService).join();
        assertTrue(streamsInBucket.contains(BucketStore.getScopedStreamName(scope, stream1)));
        assertEquals(1, streamsInBucket.size());

        List<String> streams = getAllStreams(serviceType);
        assertEquals(1, streams.size());
        
        String stream2 = "stream2";
        String stream3 = "stream3";
        bucketStore.addStreamToBucketStore(serviceType, scope, stream2, executorService).join();
        bucketStore.addStreamToBucketStore(serviceType, scope, stream3, executorService).join();

        streams = getAllStreams(serviceType);

        assertEquals(3, streams.size());

        bucketStore.removeStreamFromBucketStore(serviceType, scope, stream2, executorService).join();

        streams = getAllStreams(serviceType);

        assertEquals(2, streams.size());
        assertTrue(streams.contains(NameUtils.getScopedStreamName(scope, stream1)));
        assertTrue(streams.contains(NameUtils.getScopedStreamName(scope, stream3)));
        assertFalse(streams.contains(NameUtils.getScopedStreamName(scope, stream2)));

        Map<String, Set<Integer>> bucketMap = bucketStore.getBucketControllerMap(serviceType).join();
        assertEquals(0, bucketMap.size());
        Map<String, Set<Integer>> newBucketMap = new HashMap<>();
        //update empty bucket map
        bucketStore.updateBucketControllerMap(newBucketMap, serviceType).join();
        bucketMap = bucketStore.getBucketControllerMap(serviceType).join();
        assertEquals(0, bucketMap.size());
        //update new bucket map with values
        newBucketMap.put("host1", Set.of(1, 2, 3));
        newBucketMap.put("host2", Set.of(3, 4, 5));
        bucketStore.updateBucketControllerMap(newBucketMap, serviceType).join();
        bucketMap = bucketStore.getBucketControllerMap(serviceType).join();
        assertEquals(2, bucketMap.size());
        //get bucket service for particular host
        Set<Integer> bucketServices = bucketStore.getBucketsForController("host1", serviceType).join();
        assertEquals(3, bucketServices.size());
        assertEquals(Set.of(1, 2, 3), bucketServices);
        //remove host
        newBucketMap.remove("host2");
        bucketStore.updateBucketControllerMap(newBucketMap, serviceType).join();
        bucketMap = bucketStore.getBucketControllerMap(serviceType).join();
        assertEquals(1, bucketMap.size());
    }

    private List<String> getAllStreams(BucketStore.ServiceType serviceType) {
        return Futures.allOfWithResults(
                IntStream.range(0, bucketStore.getBucketCount(serviceType))
                         .boxed()
                         .map(bucket -> bucketStore.getStreamsForBucket(serviceType, bucket, executorService))
                         .collect(Collectors.toList())).join()
                      .stream().flatMap(Collection::stream).collect(Collectors.toList());
    }
}
