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
package io.pravega.test.integration.demo;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.client.stream.mock.MockStreamManager;
import lombok.Cleanup;

public class StartLocalService {
    
    static final int SERVICE_PORT = 6000;
    static final String SCOPE = "Scope";
    static final String STREAM_NAME = "Foo";

    public static void main(String[] args) throws Exception {
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store);
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, StartLocalService.SERVICE_PORT,
                store, tableStore, serviceBuilder.getLowPriorityExecutor(), indexAppendProcessor);
        server.startListening();
        
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, "localhost", StartLocalService.SERVICE_PORT);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, null);
        
        Thread.sleep(60000);
        System.exit(0);
    }
}
