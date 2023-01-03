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
import org.junit.Before;

public class SecureStreamMetadataTasksTest extends StreamMetadataTasksTest {
    @Override
    @Before
    public void setup() throws Exception {
        this.authEnabled = true;
        super.setup();
    }

    @Override
    StreamMetadataStore getStore() {
        return StreamStoreFactory.createInMemoryStore();
    }

    @Override
    KVTableMetadataStore getKvtStore() {
        return KVTableStoreFactory.createInMemoryStore(getStore(), executor);
    }
}
