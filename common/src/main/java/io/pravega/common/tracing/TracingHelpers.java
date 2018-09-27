/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.tracing;

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
import lombok.extern.slf4j.Slf4j;

/**
 * This class contains utilities to intercept RPC calls the server and client sides, as well as to track incoming
 * requests for intra-component tracing.
 */
@Slf4j
public class TracingHelpers {

    private static final String REQUEST_DESCRIPTOR = "requestDescriptor";
    private static final String REQUEST_ID = "requestId";
    public static final CallOptions.Key<String> REQUEST_DESCRIPTOR_CALL_OPTION = CallOptions.Key.of(REQUEST_DESCRIPTOR, "");
    public static final CallOptions.Key<String> REQUEST_ID_CALL_OPTION = CallOptions.Key.of(REQUEST_ID, "");
    private static final Metadata.Key<String> REQUEST_DESCRIPTOR_HEADER = Metadata.Key.of(REQUEST_DESCRIPTOR, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> REQUEST_ID_HEADER = Metadata.Key.of(REQUEST_ID, Metadata.ASCII_STRING_MARSHALLER);

    public static ClientInterceptor getClientInterceptor() {
        return new FishTaggingClientInterceptor();
    }

    public static ServerInterceptor getServerInterceptor() {
        return new FishTaggingServerInterceptor();
    }

    /**
     * This interceptor is intended to get request tags from call options and attach them to the RPC request.
     */
    private static class FishTaggingClientInterceptor implements ClientInterceptor {

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Get call options previously set by the client and attach that information to the RPC request.
                    final String requestDescriptor = callOptions.getOption(REQUEST_DESCRIPTOR_CALL_OPTION);
                    final String requestId = callOptions.getOption(REQUEST_ID_CALL_OPTION);

                    if (requestDescriptor != null && requestId != null && !requestDescriptor.isEmpty() && !requestId.isEmpty()) {
                        headers.put(REQUEST_DESCRIPTOR_HEADER, requestDescriptor);
                        headers.put(REQUEST_ID_HEADER, requestId);
                        log.info("[requestId={}] Tagging RPC request {}.", requestId, requestDescriptor);
                    } else {
                        log.warn("Not tagging request {}: Call options not containing request tags.", method.getFullMethodName());
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
    private static class FishTaggingServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, final Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
            // Check if this RPC has tags to track request (e.g., older clients).
            if (requestHeaders != null && requestHeaders.containsKey(REQUEST_DESCRIPTOR_HEADER) && requestHeaders.containsKey(REQUEST_ID_HEADER)) {
                RequestTag requestTag = new RequestTag(requestHeaders.get(REQUEST_DESCRIPTOR_HEADER), Long.valueOf(requestHeaders.get(REQUEST_ID_HEADER)));
                RequestTracker.getInstance().trackRequest(requestTag);
                log.info("[requestId={}] Received tag from RPC request {}.", requestHeaders.get(REQUEST_ID_HEADER), requestHeaders.get(REQUEST_DESCRIPTOR_HEADER));
            } else {
                log.info("No tags provided for call {} in headers: {}.", call.getMethodDescriptor().getFullMethodName(), requestHeaders);
            }

            return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                @Override
                public void sendHeaders(Metadata responseHeaders) {
                    super.sendHeaders(responseHeaders);
                }
            }, requestHeaders);
        }
    }
}
