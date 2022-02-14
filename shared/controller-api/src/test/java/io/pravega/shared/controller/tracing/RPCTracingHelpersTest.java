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
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.pravega.common.tracing.RequestTracker;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test to check the correct management of tracing request headers by the client/server interceptors.
 */
public class RPCTracingHelpersTest {

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
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
        @Cleanup("shutdown")
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

    @Test
    public void toSanitizeStringReplacesAuthHeaderInTheMiddle() {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("firstHeader", Metadata.ASCII_STRING_MARSHALLER), "dummy-value");

        // dXNlcm5hbWU6c3VwZXItc2VjcmV0LXBhc3N3b3Jk is base64 encoded value of `test-user:super-secret-password`
        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==");
        metadata.put(Metadata.Key.of("lastHeader", Metadata.ASCII_STRING_MARSHALLER),
                "dummy-value");

        assertTrue(metadata.toString().contains("authorization=Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==,"));

        String sanitizedString = RPCTracingHelpers.toSanitizedString(metadata);
        assertFalse(sanitizedString.contains("authorization=Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==,"));
        assertTrue(sanitizedString.contains("authorization=xxxxx,"));
    }

    @Test
    public void toSanitizeStringReplacesAuthHeaderInTheEnd() {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("first", Metadata.ASCII_STRING_MARSHALLER), "dummy-value");
        metadata.put(Metadata.Key.of("middle", Metadata.ASCII_STRING_MARSHALLER),
                "dummy-value");

        // dXNlcm5hbWU6c3VwZXItc2VjcmV0LXBhc3N3b3Jk is base64 encoded value of `test-user:super-secret-password`
        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==");

        assertTrue(metadata.toString().contains("authorization=Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==)"));
        String sanitizedString = RPCTracingHelpers.toSanitizedString(metadata);
        assertFalse(sanitizedString.contains("authorization=Basic dGVzdC11c2VyOnN1cGVyLXNlY3JldC1wYXNzd29yZA==)"));
        assertTrue(sanitizedString.contains("authorization=xxxxx)"));
    }

    @Test
    public void toSanitizeStringReplacesNothingIfAuthorizationHeaderIsMissing() {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("first", Metadata.ASCII_STRING_MARSHALLER), "dummy-value");
        metadata.put(Metadata.Key.of("middle", Metadata.ASCII_STRING_MARSHALLER), "dummy-value");
        metadata.put(Metadata.Key.of("last", Metadata.ASCII_STRING_MARSHALLER), "dummy-value");

        assertEquals(metadata.toString(), RPCTracingHelpers.toSanitizedString(metadata));
    }

    @Test
    public void toSanitizeStringReturnsNullIfMetadataIsNull() {
        assertEquals("null", RPCTracingHelpers.toSanitizedString(null));
    }
}
