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

import io.pravega.common.lang.Int96;
import io.pravega.controller.store.VersionedMetadata;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Zookeeper based counter tests.
 */
public class ZKCounterTest extends Int96CounterTest {

    @Override
    public void setupStore() {
        zkStoreHelper.createZNodeIfNotExist("/store/scope").join();
    }

    @Override
    Int96Counter getInt96Counter() {
        return spy(new ZkInt96Counter(zkStoreHelper));
    }

    @Override
    void mockCounterValue() {
        // set range in store to have lsb = Long.Max - 100
        VersionedMetadata<Int96> data = new VersionedMetadata<>(new Int96(0, Long.MAX_VALUE - 100), null);
        doReturn(CompletableFuture.completedFuture(data)).when(zkStoreHelper).getData(eq(ZkInt96Counter.COUNTER_PATH), any());
    }
}
