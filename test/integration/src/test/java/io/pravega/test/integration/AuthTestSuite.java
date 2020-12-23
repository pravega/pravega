/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.security.auth.DelegationTokenProviderFactoryTest;
import io.pravega.client.security.auth.JwtTokenProviderImplTest;
import io.pravega.client.segment.impl.AsyncSegmentInputStreamTest;
import io.pravega.client.segment.impl.ConditionalOutputStreamTest;
import io.pravega.client.segment.impl.SegmentOutputStreamTest;
import io.pravega.client.state.impl.RevisionedStreamClientTest;
import io.pravega.client.stream.impl.EventStreamReaderTest;
import io.pravega.client.stream.impl.SegmentSelectorTest;
import io.pravega.segmentstore.server.host.delegationtoken.TokenVerifierImplTest;
import io.pravega.segmentstore.server.host.handler.AppendProcessorAuthFailedTest;
import io.pravega.segmentstore.server.host.handler.AppendProcessorTest;
import io.pravega.segmentstore.server.host.handler.PravegaRequestProcessorAuthFailedTest;
import io.pravega.segmentstore.server.host.handler.PravegaRequestProcessorTest;
import io.pravega.shared.protocol.netty.WireCommandsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // Client tests
        ConditionalOutputStreamTest.class,
        SegmentOutputStreamTest.class,
        DelegationTokenProviderFactoryTest.class,
        JwtTokenProviderImplTest.class,
        EventStreamReaderTest.class,
        RevisionedStreamClientTest.class,
        SegmentSelectorTest.class,
        ConditionalOutputStreamTest.class,
        AsyncSegmentInputStreamTest.class,

        // Shared tests
        WireCommandsTest.class,

        // Segment store tests
        PravegaRequestProcessorAuthFailedTest.class,
        TokenVerifierImplTest.class,
        AppendProcessorAuthFailedTest.class,
        PravegaRequestProcessorAuthFailedTest.class,
        AppendProcessorTest.class,
        PravegaRequestProcessorTest.class,

        // Integration tests
        ControllerGrpcListStreamsTest.class,
        DelegationTokenTest.class,
        BatchClientAuthTest.class,
        AppendTest.class,
        AppendReconnectTest.class,
        ReadTest.class,
})

public class AuthTestSuite {
}
