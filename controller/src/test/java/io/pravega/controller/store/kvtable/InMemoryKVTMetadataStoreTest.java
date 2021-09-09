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
package io.pravega.controller.store.kvtable;

import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;

/**
 * In-memory stream metadata store tests.
 */
public class InMemoryKVTMetadataStoreTest extends KVTableMetadataStoreTest {

    @Override
    public void setupStore() throws Exception {
        this.streamStore = StreamStoreFactory.createInMemoryStore();
        store = KVTableStoreFactory.createInMemoryStore(this.streamStore, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
    }

    @Override
    Controller.CreateScopeStatus createScope(String scopeName) throws Exception {
        return streamStore.createScope(scopeName, null, executor).get();
    }
}
