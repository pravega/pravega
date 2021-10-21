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
 * This is to test Storage in No-Op mode for user segments only.
 *
 * In this test the underlying userStorage is provided to store user segments,
 * in order to verified the No-Op for user data is done correctly.
 * Because no data is lost, then all the operations should succeed.
 *
 */
public class NoOpStorageUserDataWriteReadTests extends StorageTestBase {

    private SyncStorage systemStorage;
    private SyncStorage userStorage;
    private StorageExtraConfig config;

    @Before
    public void setUp() {
        //Inside this test only user segments are being tested.
        setTestingSystemSegment(false);
        systemStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        userStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new NoOpStorage(config, systemStorage, userStorage), executorService());
    }

    @Override
    public void testFencing() {
    }

    @Override
    public void testListSegmentsWithOneSegment() {
    }

    @Override
    public void testListSegmentsNextNoSuchElementException() {
    }
}
