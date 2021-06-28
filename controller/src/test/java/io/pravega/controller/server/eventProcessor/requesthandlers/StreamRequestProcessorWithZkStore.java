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
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

public class StreamRequestProcessorWithZkStore extends StreamRequestProcessorTest {
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource();
    private StreamMetadataStore store;

    @Before
    public void setUp() throws Exception {

        store = StreamStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executorService());
    }

    @After
    public void tearDown() throws Exception {
        store.close();
    }

    @Override
    StreamMetadataStore getStore() {
        return store;
    }
}
