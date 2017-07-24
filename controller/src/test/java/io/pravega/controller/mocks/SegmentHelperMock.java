/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class SegmentHelperMock {
    private static final int SERVICE_PORT = 12345;

    public static SegmentHelper getSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper());

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).deleteSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).abortTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).commitTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyInt(), any(), any());
        return helper;
    }

    public static SegmentHelper getFailingTxnOpsSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper());

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).deleteSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(FutureHelpers.failedFuture(new IllegalStateException())).when(helper).abortTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(FutureHelpers.failedFuture(new IllegalStateException())).when(helper).commitTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyInt(), any(), any());

        doReturn(FutureHelpers.failedFuture(new IllegalStateException())).when(helper).createTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        return helper;
    }
}
