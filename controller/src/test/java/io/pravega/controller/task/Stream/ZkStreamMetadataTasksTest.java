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
package io.pravega.controller.task.Stream;

import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;

public class ZkStreamMetadataTasksTest extends StreamMetadataTasksTest {
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(zkClient, executor);
    }

    @Override
    KVTableMetadataStore getKvtStore() {
        return KVTableStoreFactory.createZKStore(zkClient, executor);
    }

    @Test
    public void addSubscriberTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Test
    public void removeSubscriberTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Override
    @Test
    public void updateSubscriberStreamCutTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Override
    @Test
    public void readerGroupsTest() throws InterruptedException, ExecutionException {
        // skip ZK tests
        assertTrue(true);
    }

    @Test
    @Override
    public void consumptionBasedRetentionSizeLimitTest() {
        // no op
    }

    @Test
    @Override
    public void consumptionBasedRetentionTimeLimitTest() {
        // no op
    }
    
    @Test
    @Override
    public void consumptionBasedRetentionWithScale() {
        // no op
    }
    
    @Test
    @Override
    public void consumptionBasedRetentionWithScale2() {
        // no op
    }

    @Test
    @Override
    public void consumptionBasedRetentionWithNoBounds() {
        // no op
    }

    @Test
    @Override
    public void consumptionBasedRetentionWithNoSubscriber() {
        // no op
    }

    @Test
    @Override
    public void consumptionBasedRetentionSizeLimitWithOverlappingMinTest() {
        // no op
    }

    @Test
    @Override
    public void consumptionBasedRetentionTimeLimitWithOverlappingMinTest() {
        // no op
    }

    @Test
    @Override
    public void sizeBasedRetentionStreamTest() {
        // no op
    }

    @Test
    @Override
    public void readerGroupFailureTests() {
        // no op
    }
}
