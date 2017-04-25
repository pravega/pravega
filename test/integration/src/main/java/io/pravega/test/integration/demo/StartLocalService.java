/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.demo;

import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.stream.mock.MockStreamManager;
import lombok.Cleanup;

public class StartLocalService {
    
    static final int PORT = 9090;
    static final String SCOPE = "Scope";
    static final String STREAM_NAME = "Foo";

    public static void main(String[] args) throws Exception {
        @Cleanup
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, PORT, store);
        server.startListening();
        
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, "localhost", StartLocalService.PORT);
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, STREAM_NAME, null);
        
        Thread.sleep(60000);
        System.exit(0);
    }
}
