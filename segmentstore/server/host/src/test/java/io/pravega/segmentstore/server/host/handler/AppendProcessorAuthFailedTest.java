/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.pravega.auth.TokenException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AppendProcessorAuthFailedTest {

    private AppendProcessor processor;
    private ServerConnection connection;

    @Before
    public void setUp() throws Exception {
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        connection = mock(ServerConnection.class);

        processor = new AppendProcessor(store, connection, new FailingRequestProcessor(),
                (resource, token, expectedLevel) -> {
                    throw new TokenException("Token verification failed.");
                });
    }

    @Test
    public void setupAppend() {
        processor.setupAppend(new WireCommands.SetupAppend(100L,
                UUID.randomUUID(), "segment", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }
}