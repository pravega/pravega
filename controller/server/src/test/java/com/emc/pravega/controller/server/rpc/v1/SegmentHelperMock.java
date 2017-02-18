/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.controller.stream.api.v1.NodeUri;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class SegmentHelperMock {

    public static void init() {
        SegmentHelper singleton = spy(SegmentHelper.getSegmentHelper());

        SegmentHelper.setSingleton(singleton);
        doReturn(new NodeUri("localhost", 12345)).when(singleton).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(singleton).sealSegment(
                anyString(), anyString(), anyInt(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(singleton).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(singleton).abortTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(singleton).commitTransaction(
                anyString(), anyString(), anyInt(), any(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(singleton).sealSegment(
                anyString(), anyString(), anyInt(), any(), any());
    }
}
