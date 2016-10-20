package com.emc.pravega.demo;

import java.time.Duration;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.mocks.InMemoryServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.mock.MockStreamManager;

import lombok.Cleanup;

public class StartLocalService {
    
    static final int PORT = 9090;
    static final String SCOPE = "Scope";
    static final String STREAM_NAME = "Foo";

    public static void main(String[] args) throws Exception {
        @Cleanup
        InMemoryServiceBuilder serviceBuilder = new InMemoryServiceBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.getContainerManager().initialize(Duration.ofMinutes(1)).get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, PORT, store);
        server.startListening();
        
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, "localhost", StartLocalService.PORT);
        streamManager.createStream(STREAM_NAME, null);
        
        Thread.sleep(60000);
        System.exit(0);
    }
}
