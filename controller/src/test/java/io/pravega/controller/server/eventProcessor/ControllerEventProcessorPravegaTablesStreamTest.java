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
package io.pravega.controller.server.eventProcessor;

import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorPravegaTablesStreamTest extends ControllerEventProcessorTest {
    @Override
    StreamMetadataStore createStore() {
        return StreamStoreFactory.createPravegaTablesStore(
                SegmentHelperMock.getSegmentHelperMockForTables(executor), GrpcAuthHelper.getDisabledAuthHelper(), PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }
}
