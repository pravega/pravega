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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

@Slf4j
public class TracingHelpers {

    private static final String REQUEST_DESCRIPTOR = "requestDescriptor";
    private static final String REQUEST_ID = "requestId";
    private static final String THREAD_ID = "threadId";
    private static final Metadata.Key<String> CUSTOM_HEADER_KEY = Metadata.Key.of(REQUEST_DESCRIPTOR, Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> CUSTOM_HEADER_VALUE = Metadata.Key.of(REQUEST_ID, Metadata.ASCII_STRING_MARSHALLER);

    public static ClientInterceptor getClientInterceptor() {
        return new FishTaggingClientInterceptor();
    }

    public static ServerInterceptor getServerInterceptor() {
        return new FishTaggingServerInterceptor();
    }

    /**
     * Attach the request id and the information to create a request descriptor to the MDC. This information will be
     * used by the FishTaggingClientInterceptor to set the appropriate headers to the RPC request.
     *
     * @param requestId Request id for this RPC.
     * @param requestInfo Series of string arguments to identify the RPC call.
     */
    public static void attachTagToRPCRequest(long requestId, String...requestInfo) {
        MDC.put(REQUEST_DESCRIPTOR, RequestTracker.createRPCRequestDescriptor(requestInfo));
        MDC.put(REQUEST_ID, String.valueOf(requestId));
        MDC.put(THREAD_ID, String.valueOf(Thread.currentThread().getId()));
    }

    /**
     * This method first attempts to load from the MDC the values set from RPC headers by FishTaggingServerInterceptor.
     * However, if we work with clients that do not attach tags to RPCs, then we provide tags initialized at the server
     * side. In the worst case, we will have the ability of tracking a request from the RPC server onwards.
     *
     * @param requestId Alternative request id for this RPC in the case there is no request id in headers.
     * @param requestInfo Alternative descriptor to identify the RPC call in the case there is no descriptor in headers.
     * @return Request tag formed either from the RPC request headers or from arguments given.
     */
    public static RequestTag getOrInitializeRequestTags(long requestId, String...requestInfo) {
        Map<String, String> mdcCopy = MDC.getCopyOfContextMap();
        RequestTag requestTag;
        if (mdcCopy != null && mdcCopy.containsKey(REQUEST_ID) && mdcCopy.get(THREAD_ID).equals(String.valueOf(Thread.currentThread().getId()))) {
            requestTag = new RequestTag(mdcCopy.get(REQUEST_DESCRIPTOR), Long.valueOf(mdcCopy.get(REQUEST_ID)));
        } else {
            log.warn("Request tags not found in MDC {}, generating new ones for this request: requestId={}, descriptor={}.", mdcCopy, requestId,
                    RequestTracker.createRPCRequestDescriptor(requestInfo));
            requestTag = new RequestTag(RequestTracker.createRPCRequestDescriptor(requestInfo), requestId);
        }
        log.info("[requestId={}] Getting tags from request {}.", requestTag.getRequestId(), requestTag.getRequestDescriptor());
        cleanMDCInfo();
        return requestTag;
    }

    private static void cleanMDCInfo() {
        MDC.clear();
    }

    /**
     * This interceptor is intended to get request tags from MDC and attach them to the RPC request.
     */
    private static class FishTaggingClientInterceptor implements ClientInterceptor {

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Get the MDC context previously set by the client and attach that information to the RPC request.
                    Map<String, String> requestInfo = MDC.getCopyOfContextMap();
                    if (requestInfo != null && requestInfo.get(THREAD_ID).equals(String.valueOf(Thread.currentThread().getId()))) {
                        headers.put(CUSTOM_HEADER_KEY, String.valueOf(requestInfo.get(REQUEST_DESCRIPTOR)));
                        headers.put(CUSTOM_HEADER_VALUE, String.valueOf(requestInfo.get(REQUEST_ID)));
                        log.info("[requestId={}] Tagging RPC request {} at thread {}.", requestInfo.get(REQUEST_ID),
                                requestInfo.get(REQUEST_DESCRIPTOR), Thread.currentThread().getId());
                    } else {
                        log.warn("Not tagging request: MDC {} not containing proper information or executed by another thread {}.",
                                requestInfo, Thread.currentThread());
                    }

                    cleanMDCInfo();
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
     * This server interceptor is intended to get RPC tags from RPC headers and set the them in the MDC.
     */
    private static class FishTaggingServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, final Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
            // Check if this RPC has tags to track request (e.g., older clients).
            if (requestHeaders != null && requestHeaders.containsKey(CUSTOM_HEADER_KEY) && requestHeaders.containsKey(CUSTOM_HEADER_VALUE)) {
                MDC.put(REQUEST_DESCRIPTOR, requestHeaders.get(CUSTOM_HEADER_KEY));
                MDC.put(REQUEST_ID, requestHeaders.get(CUSTOM_HEADER_VALUE));
                MDC.put(THREAD_ID, String.valueOf(Thread.currentThread().getId()));
                log.info("[requestId={}] Received tag from RPC request {} in thread {}.", MDC.get(REQUEST_ID), MDC.get(REQUEST_DESCRIPTOR), Thread.currentThread().getId());
            } else {
                log.info("No tags provided in headers: {}.", requestHeaders);
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
