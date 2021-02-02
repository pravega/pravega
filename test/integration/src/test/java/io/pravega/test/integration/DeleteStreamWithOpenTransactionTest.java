/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

@Slf4j
public class DeleteStreamWithOpenTransactionTest {
    @Test(timeout = 30000)
    public void testDeleteStreamWithOpenTransaction() throws Exception {
        @Cleanup("stopAllServices")
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices(1);

        final String scope = setupUtils.getScope();
        final ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(setupUtils.getControllerUri())
                .build();;

        @Cleanup
        final StreamManager streamManager = StreamManager.create(clientConfig);

        streamManager.createScope(scope);
        final String stream = "test";
        streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());

        @Cleanup
        final EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        @Cleanup
        final TransactionalEventStreamWriter<String> writer =
                clientFactory.createTransactionalEventWriter("writerId1", stream, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // Transactions 0-4 will be opened, written, flushed, committed.
        // Transactions 5-6 will be opened, written, flushed.
        // Transactions 7-8 will be opened, written.
        // Transactions 9-10 will be opened.
        for (int i = 0 ; i < 11 ; i++) {
            final Transaction<String> txn = writer.beginTxn();
            log.info("i={}, txnId={}", i, txn.getTxnId());
            if (i <= 8) {
                txn.writeEvent("foo");
            }
            if (i <= 6) {
                txn.flush();
            }
            if (i <= 4) {
                txn.commit();
            }
        }
        boolean sealed = streamManager.sealStream(scope, stream);
        Assert.assertTrue(sealed);
        boolean deleted = streamManager.deleteStream(scope, stream);
        Assert.assertTrue(deleted);
    }
}