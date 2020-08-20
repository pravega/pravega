/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import com.beust.jcommander.internal.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.SingleRoutingKeyTransaction;
import io.pravega.client.stream.SingleRoutingKeyTransactionWriter;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.common.util.Retry;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestingServerStarter;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@Slf4j
public class SingleRoutingKeyDemo {
    static final StreamConfiguration CONFIG = StreamConfiguration.builder()
                                                                 .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 3))
                                                                 .build();

    public static void main(String[] args) throws Exception {
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build();
        StreamManager streamManager = StreamManager.create(clientConfig);
        String scope = "scope";
        streamManager.createScope(scope);
        String stream = "stream";
        streamManager.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();

        EventStreamWriter<String> test = clientFactory.createEventWriter(
                "test", serializer, EventWriterConfig.builder().build());

        List<String> batch = Lists.newArrayList("a", "b", "c");
        test.writeEvents("routingkey", batch).get();

        SingleRoutingKeyTransactionWriter<String> txnWriter = clientFactory.createSingleRoutingKeyWriter(stream, "routingkey", serializer);
        SingleRoutingKeyTransaction<String> txn = txnWriter.beginTxn();
        txn.writeEvent("a");
        txn.writeEvent("b");
        txn.writeEvent("c");
        txn.commit();
    }
}
