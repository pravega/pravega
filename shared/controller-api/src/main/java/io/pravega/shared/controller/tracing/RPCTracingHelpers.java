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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import org.slf4j.LoggerFactory;

/**
 * This class contains utilities to intercept RPC calls the server and client sides, as well as to track incoming
 * requests for intra-component tracing.
 */
public final class RPCTracingHelpers {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(RPCTracingHelpers.class));

    private static final String REQUEST_DESCRIPTOR = "requestDescriptor";
    private static final String REQUEST_ID = "requestId";
    public static final CallOptions.Key<String> REQUEST_DESCRIPTOR_CALL_OPTION = CallOptions.Key.createWithDefault(REQUEST_DESCRIPTOR, "");
    public static final CallOptions.Key<String> REQUEST_ID_CALL_OPTION = CallOptions.Key.createWithDefault(REQUEST_ID, "");
    static final Metadata.Key<String> DESCRIPTOR_HEADER = Metadata.Key.of(REQUEST_DESCRIPTOR, Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> ID_HEADER = Metadata.Key.of(REQUEST_ID, Metadata.ASCII_STRING_MARSHALLER);

    public static ClientInterceptor getClientInterceptor() {
        return new TaggingClientInterceptor();
    }

    public static ServerInterceptor getServerInterceptor(RequestTracker requestTracker) {
        return new TaggingServerInterceptor(requestTracker);
    }

    @VisibleForTesting
    static String toSanitizedString(Metadata headers) {
        return headers == null ? "null" :
                headers.toString().replaceAll("authorization=.*(?=,)|authorization=.*(?=\\))", "authorization=xxxxx");
    }

    /**
     * This interceptor is intended to get request tags from call options and attach them to the RPC request.
     */
    private static class TaggingClientInterceptor implements ClientInterceptor {

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                   CallOptions callOptions, Channel next) {

            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Get call options previously set by the client and attach that information to the RPC request.
                    final String requestDescriptor = callOptions.getOption(REQUEST_DESCRIPTOR_CALL_OPTION);
                    final String requestId = callOptions.getOption(REQUEST_ID_CALL_OPTION);

                    if (requestDescriptor != null && requestId != null && !requestDescriptor.isEmpty() && !requestId.isEmpty()) {
                        headers.put(DESCRIPTOR_HEADER, requestDescriptor);
                        headers.put(ID_HEADER, requestId);
                        log.debug(Long.parseLong(requestId), "Tagging RPC request {}.",
                                requestDescriptor);
                    } else {
                        log.debug("Request not tagged {}: Call options not containing request tags.", method.getFullMethodName());
                    }

                    super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        @Override
                        public void onHeaders(Metadata headers) {
                            super.onHeaders(headers);
                        }
                    }, headers);
                }
            };
        }
    }

    /**
     * This server interceptor is intended to get RPC tags from RPC headers and set the them in the request tracker.
     */
    private static class TaggingServerInterceptor implements ServerInterceptor {

        private final RequestTracker requestTracker;

        public TaggingServerInterceptor(RequestTracker requestTracker) {
            this.requestTracker = requestTracker;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, final Metadata headers,
                                                                     ServerCallHandler<ReqT, RespT> next) {
            // Check if this RPC has tags to track request (e.g., older clients).
            if (headers != null && headers.containsKey(DESCRIPTOR_HEADER) && headers.containsKey(ID_HEADER)) {
                RequestTag requestTag = new RequestTag(headers.get(DESCRIPTOR_HEADER), Long.parseLong(headers.get(ID_HEADER)));
                requestTracker.trackRequest(requestTag);
                log.debug(requestTag.getRequestId(), "Received tag from RPC request {}.",
                        requestTag.getRequestDescriptor());
            } else {
                log.debug("No tags provided for call {} in headers: {}.", call.getMethodDescriptor().getFullMethodName(),
                        toSanitizedString(headers));
            }

            return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                @Override
                public void sendHeaders(Metadata responseHeaders) {
                    super.sendHeaders(responseHeaders);
                }
            }, headers);
        }
    }
}
