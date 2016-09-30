package com.emc.pravega.stream.mock;

import java.util.concurrent.ConcurrentHashMap;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.ScalingPolicy.Type;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamManager;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

public class MockStreamManager implements StreamManager {
    
    private final String scope;
    private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
    private final ConnectionFactoryImpl connectionFactory;
    private final MockController controller;
    
    public MockStreamManager(String scope, String endpoint, int port) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = new MockController(endpoint, port, connectionFactory);
    }

    @Override
    public Stream createStream(String streamName, StreamConfiguration config) {
        if (config == null) {
            config = new StreamConfigurationImpl(scope, streamName, new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, 1));
        }
        Stream stream = createStreamHelper(streamName, config);
        return stream;
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller
            .createStream(new StreamConfigurationImpl(scope, streamName, config.getScalingingPolicy())),
                                             RuntimeException::new);
        Stream stream = new StreamImpl(scope, streamName, config, controller, connectionFactory);
        created.put(streamName, stream);
        return stream;
    }

    @Override
    public Stream getStream(String streamName) {
        return created.get(streamName);
    }

    @Override
    public void close() {

    }
    
    public Position getInitialPosition(String stream) {
        return controller.getInitialPosition(scope, stream);
    }
    
}
