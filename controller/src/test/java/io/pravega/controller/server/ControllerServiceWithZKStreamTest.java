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
package io.pravega.controller.server;

import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Tests for ControllerService With ZK Stream Store
 */
@Slf4j
public class ControllerServiceWithZKStreamTest extends ControllerServiceWithStreamTest {
    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createZKStore(zkClient, executor);
    }

    @Override
    KVTableMetadataStore getKVTStore() {
        return KVTableStoreFactory.createZKStore(zkClient, executor);
    }

    @Override
    public void createReaderGroupTest() throws Exception {
        // do nothing...
    }
}
