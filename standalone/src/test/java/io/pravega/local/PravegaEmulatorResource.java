/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class contains the rules to start and stop LocalPravega / standalone.
 * This resource can be configured for a test using @ClassRule and it will ensure standalone is started once
 * for all the methods in a test and is shutdown once all the tests methods have completed execution.
 */
@Slf4j
public class PravegaEmulatorResource extends ExternalResource {
    final boolean restEnabled = false; // disable REST server for the current tests.
    final boolean authEnabled;
    final boolean tlsEnabled;
    final LocalPravegaEmulator pravega;

    public PravegaEmulatorResource(boolean authEnabled, boolean tlsEnabled) {
        this.authEnabled = authEnabled;
        this.tlsEnabled = tlsEnabled;
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(TestUtils.getAvailableListenPort())
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(restEnabled)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

        // Since the server is being built right here, avoiding delegating these conditions to subclasses via factory
        // methods. This is so that it is easy to see the difference in server configs all in one place. This is also
        // unlike the ClientConfig preparation which is being delegated to factory methods to make their preparation
        // explicit in the respective test classes.

        if (authEnabled) {
            emulatorBuilder.passwdFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                    .userName(SecurityConfigDefaults.AUTH_ADMIN_USERNAME)
                    .passwd(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD);
        }
        if (tlsEnabled) {
            emulatorBuilder.certFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                    .keyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                    .jksKeyFile(SecurityConfigDefaults.TLS_SERVER_KEYSTORE_PATH)
                    .jksTrustFile(SecurityConfigDefaults.TLS_CLIENT_TRUSTSTORE_PATH)
                    .keyPasswordFile(SecurityConfigDefaults.TLS_PASSWORD_PATH);
        }

        pravega = emulatorBuilder.build();
    }

    protected void before() throws Exception {
        pravega.start();
    }

    @SneakyThrows
    protected void after() {
        pravega.close();
    }

    public void testWriteAndReadAnEventWithValidClientConfig(String scope, String streamName, String message, ClientConfig clientConfig) throws Exception {
        int numSegments = 1;

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);
        assertTrue("Failed to create scope", isScopeCreated);
        boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);
        log.info("write event");
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Write an event to the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent(message).get();
        log.debug("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);

        // Now, read the event from the stream.

        String readerGroup = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .disableAutomaticCheckpoints()
                .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(
                "readerId", readerGroup,
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());
        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readMessage = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }
}


