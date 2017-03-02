/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.mocks;

import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.stream.api.v1.NodeUri;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class SegmentHelperMock {

    public static SegmentHelper getSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper());

        doReturn(new NodeUri("localhost", 12345)).when(helper).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).abortTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).commitTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyInt(), any(), any());
        return helper;
    }
}
