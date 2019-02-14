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

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class SegmentHelperMock {
    private static final int SERVICE_PORT = 12345;

    public static SegmentHelper getSegmentHelperMock(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        SegmentHelper helper = spy(new SegmentHelper(hostControllerStore, clientCF, authHelper));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).truncateSegment(
                anyString(), anyString(), anyLong(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(new WireCommands.StreamSegmentInfo(0L, "", true, true, false, 0L, 0L, 0L))).when(helper).getSegmentInfo(
                anyString(), anyString(), anyLong());

        return helper;
    }

    public static SegmentHelper getFailingSegmentHelperMock(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        SegmentHelper helper = spy(new SegmentHelper(hostControllerStore, clientCF, authHelper));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), anyLong());

        return helper;
    }
}
