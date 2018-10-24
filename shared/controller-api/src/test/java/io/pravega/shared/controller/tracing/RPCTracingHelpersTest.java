/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.controller.tracing;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyChannelBuilder;
import io.pravega.common.tracing.RequestTracker;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test to check the correct management of tracing request headers by the client/server interceptors.
 */
@Slf4j
public class RPCTracingHelpersTest {

    @Test
    public void testInterceptors() {
        String requestDescriptor = "createStream-myScope-myStream";
        long requestId = 1234L;
        ClientInterceptor clientInterceptor = RPCTracingHelpers.getClientInterceptor();
        RequestTracker requestTracker = new RequestTracker(true);

        // Mocking RPC elements.
        MethodDescriptor.Marshaller<Object> mockMarshaller = Mockito.mock(MethodDescriptor.Marshaller.class);
        ClientCall.Listener<Object> listener = Mockito.mock(ClientCall.Listener.class);
        ServerCall serverCall = Mockito.mock(ServerCall.class);
        ServerCallHandler serverCallHandler = Mockito.mock(ServerCallHandler.class);
        ManagedChannel channel = NettyChannelBuilder.forTarget("localhost").build();
        MethodDescriptor method = MethodDescriptor.newBuilder()
                                                  .setFullMethodName("createStream")
                                                  .setType(MethodDescriptor.MethodType.UNARY)
                                                  .setRequestMarshaller(mockMarshaller)
                                                  .setResponseMarshaller(mockMarshaller)
                                                  .build();
        Mockito.when(serverCall.getMethodDescriptor()).thenReturn(method);

        // Actual elements to work with.
        CallOptions callOptions = CallOptions.DEFAULT;
        Metadata headers = new Metadata();

        // Test that headers do not contain tracing-related key/values, as call options are not set.
        clientInterceptor.interceptCall(method, callOptions, channel).start(listener, headers);
        assertFalse(headers.containsKey(RPCTracingHelpers.DESCRIPTOR_HEADER));
        assertFalse(headers.containsKey(RPCTracingHelpers.ID_HEADER));

        // Check that the server interceptor handles clients not sending tracing headers and that the cache remains clean.
        ServerInterceptor serverInterceptor = RPCTracingHelpers.getServerInterceptor(requestTracker);
        serverInterceptor.interceptCall(serverCall, headers, serverCallHandler);
        assertEquals(0, requestTracker.getNumDescriptors());

        // Add call options and check that headers are correctly set.
        callOptions = callOptions.withOption(RPCTracingHelpers.REQUEST_DESCRIPTOR_CALL_OPTION, requestDescriptor)
                                 .withOption(RPCTracingHelpers.REQUEST_ID_CALL_OPTION, String.valueOf(requestId));
        clientInterceptor.interceptCall(method, callOptions, channel).start(listener, headers);
        assertEquals(requestDescriptor, headers.get(RPCTracingHelpers.DESCRIPTOR_HEADER));
        assertEquals(requestId, Long.parseLong(headers.get(RPCTracingHelpers.ID_HEADER)));

        // Test that the server interceptor correctly sets these headers in the cache for further tracking.
        serverInterceptor.interceptCall(serverCall, headers, serverCallHandler);
        assertEquals(1, requestTracker.getNumDescriptors());
        assertEquals(requestId, requestTracker.getRequestIdFor(requestDescriptor));
    }
}
