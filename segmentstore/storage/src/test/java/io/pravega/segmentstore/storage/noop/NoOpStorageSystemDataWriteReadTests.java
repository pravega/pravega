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
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import org.junit.Before;

/**
 * This is to test Storage in No-Op mode for system segments only.
 *
 * For system segment storage, operations on both directions (write and read) should just work as before.
 * In other words, No-Op layer is totally transparent for system segments.
 */
public class NoOpStorageSystemDataWriteReadTests extends StorageTestBase {

    private SyncStorage systemStorage;
    private StorageExtraConfig config;

    @Before
    public void setUp() {
        //In this test only system segments are being tested.
        setTestingSystemSegment(true);
        systemStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new NoOpStorage(config, systemStorage, null), executorService());
    }

    /**
     * This method intentionally left blank as it's out of concern for No-Op Storage.
     * It must be here as it is defined as abstract method in super class.
     */
    @Override
    public void testFencing() {
    }

    /**
     * This method intentionally left blank as it's out of concern for No-Op Storage.
     * It must be here as it is defined as abstract method in super class.
     */
    @Override
    public void testListSegmentsWithOneSegment() {
    }

    /**
     * This method intentionally left blank as it's out of concern for No-Op Storage.
     * It must be here as it is defined as abstract method in super class.
     */
    @Override
    public void testListSegmentsNextNoSuchElementException() {
    }
}
