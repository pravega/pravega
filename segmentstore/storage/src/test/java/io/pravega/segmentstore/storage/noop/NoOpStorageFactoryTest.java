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

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.junit.Test;

public class NoOpStorageFactoryTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateSyncStorage() {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        StorageFactory factory = new NoOpStorageFactory(config, executor, new InMemoryStorageFactory(executor), null);
        factory.createSyncStorage();
    }
}
