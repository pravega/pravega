/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

<<<<<<< HEAD
import io.pravega.client.admin.KeyValueTableInfo;
=======
>>>>>>> Issue 4603: (KeyValueTables) Client Controller API (#4612)
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for LocalController.
 */
@Slf4j
public class LocalControllerTest extends ThreadPooledTestSuite {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);
    boolean authEnabled = false;

    private ControllerService mockControllerService;
    private LocalController testController;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setup() {
        this.mockControllerService = mock(ControllerService.class);
        this.testController = new LocalController(this.mockControllerService, authEnabled, "secret");
    }

    @Test
    public void testCreateScope() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.createScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createScope("scope").join());

        when(this.mockControllerService.createScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.SCOPE_EXISTS).build()));
        Assert.assertFalse(this.testController.createScope("scope").join());

        when(this.mockControllerService.createScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatus(Controller.CreateScopeStatus.Status.INVALID_SCOPE_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateScopeStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testDeleteScope() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteScope("scope").join());

        when(this.mockControllerService.deleteScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteScope("scope").join());

        when(this.mockControllerService.deleteScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatus(Controller.DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build()));
        assertThrows("Expected IllegalStateException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof IllegalStateException);

        when(this.mockControllerService.deleteScope(any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteScopeStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteScope("scope").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testCreateStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.STREAM_EXISTS).build()));
        Assert.assertFalse(this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.INVALID_STREAM_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatus(Controller.CreateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createStream(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testUpdateStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.updateStream(any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join());

        when(this.mockControllerService.updateStream(any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.updateStream(any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateStream(any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.updateStream(any(), any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.updateStream("scope", "stream", StreamConfiguration.builder().build()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testSealStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.sealStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.sealStream("scope", "stream").join());

        when(this.mockControllerService.sealStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.sealStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.STREAM_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.sealStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatus(Controller.UpdateStreamStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.sealStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.UpdateStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.sealStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testDeleteStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteStream("scope", "stream").join());

        when(this.mockControllerService.deleteStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.STREAM_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteStream("scope", "stream").join());

        when(this.mockControllerService.deleteStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatus(Controller.DeleteStreamStatus.Status.STREAM_NOT_SEALED).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.deleteStream(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteStreamStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteStream("scope", "stream").join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testScaleStream() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.checkScale(anyString(), anyString(), anyInt())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleStatusResponse.newBuilder()
                        .setStatus(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS).build()));
        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.STARTED).build()));
        Assert.assertTrue(this.testController.scaleStream(new StreamImpl("scope", "stream"),
                new ArrayList<>(), new HashMap<>(), executorService()).getFuture().join());

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED).build()));
        Assert.assertFalse(this.testController.scaleStream(new StreamImpl("scope", "stream"),
                new ArrayList<>(), new HashMap<>(), executorService()).getFuture().join());

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatus(Controller.ScaleResponse.ScaleStreamStatus.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.startScale(new StreamImpl("scope", "stream"),
                        new ArrayList<>(), new HashMap<>()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.scale(any(), any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.ScaleResponse.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.startScale(new StreamImpl("scope", "stream"),
                        new ArrayList<>(), new HashMap<>()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testGetSegmentsBetween() throws ExecutionException, InterruptedException {
        List<StreamSegmentRecord> list = new ArrayList<>();
        when(this.mockControllerService.getSegmentsBetweenStreamCuts(any())).thenReturn(
                CompletableFuture.completedFuture(list));
        Assert.assertTrue(Futures.await(this.testController.getSegments(new StreamCutImpl(new StreamImpl("scope", "stream"), Collections.emptyMap()),
                new StreamCutImpl(new StreamImpl("scope", "stream"), Collections.emptyMap()))));
    }

    @Test
<<<<<<< HEAD
    public void testCreateKeyValueTable() {
        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.createKeyValueTable("scope", "kvtable",
                KeyValueTableConfiguration.builder().partitionCount(1).build()).join());

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.TABLE_EXISTS).build()));
        Assert.assertFalse(this.testController.createKeyValueTable("scope", "kvtable",
                KeyValueTableConfiguration.builder().partitionCount(1).build()).join());

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        KeyValueTableConfiguration.builder().partitionCount(1).build()).join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.INVALID_TABLE_NAME).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        KeyValueTableConfiguration.builder().partitionCount(1).build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatus(Controller.CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND).build()));
        assertThrows("Expected IllegalArgumentException",
                () -> this.testController.createKeyValueTable("scope", "kvtable",
                        KeyValueTableConfiguration.builder().partitionCount(1).build()).join(),
                ex -> ex instanceof IllegalArgumentException);

        when(this.mockControllerService.createKeyValueTable(any(), any(), any(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(Controller.CreateKeyValueTableStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.createKeyValueTable("scope", "kvtable1",
                        KeyValueTableConfiguration.builder().partitionCount(1).build()).join(),
                ex -> ex instanceof ControllerFailureException);
    }

    @Test
    public void testGetCurrentSegmentsKeyValueTable() throws Exception {
        Controller.StreamInfo info = Controller.StreamInfo.newBuilder().setScope("scope").setStream("kvtable").build();
        Controller.SegmentId segment1 = Controller.SegmentId.newBuilder().setSegmentId(1).setStreamInfo(info).build();
        Controller.SegmentId segment2 = Controller.SegmentId.newBuilder().setSegmentId(2).setStreamInfo(info).build();
        Controller.SegmentId segment3 = Controller.SegmentId.newBuilder().setSegmentId(3).setStreamInfo(info).build();

        Controller.SegmentRange segmentRange1 = Controller.SegmentRange.newBuilder().setSegmentId(segment1).setMinKey(0.1).setMaxKey(0.3).build();
        Controller.SegmentRange segmentRange2 = Controller.SegmentRange.newBuilder().setSegmentId(segment2).setMinKey(0.4).setMaxKey(0.6).build();
        Controller.SegmentRange segmentRange3 = Controller.SegmentRange.newBuilder().setSegmentId(segment3).setMinKey(0.7).setMaxKey(1.0).build();

        List<Controller.SegmentRange> segmentsList = new ArrayList<Controller.SegmentRange>(3);
        segmentsList.add(segmentRange1);
        segmentsList.add(segmentRange2);
        segmentsList.add(segmentRange3);

        when(this.mockControllerService.getCurrentSegmentsKeyValueTable(any(), any())).thenReturn(
                CompletableFuture.completedFuture(segmentsList));
        KeyValueTableSegments segments = this.testController.getCurrentSegmentsForKeyValueTable("scope", "kvtable").get();
        Assert.assertEquals(3, segments.getSegments().size());
    }

    @Test
    public void testListKeyValueTable() throws Exception {
        List<String> tablelist = new ArrayList<String>();
        tablelist.add("kvtable1");
        Pair<List<String>, String> listOfKVTables = new ImmutablePair<>(tablelist, "");
        when(this.mockControllerService.listKeyValueTables(anyString(), anyString(), anyInt())).thenReturn(
                CompletableFuture.completedFuture(listOfKVTables));
        KeyValueTableInfo info = this.testController.listKeyValueTables("scope").getNext().get();
        Assert.assertEquals("kvtable1", info.getKeyValueTableName());
    }

    @Test
    public void testDeleteKeyValueTable() throws ExecutionException, InterruptedException {
        when(this.mockControllerService.deleteKeyValueTable(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.SUCCESS).build()));
        Assert.assertTrue(this.testController.deleteKeyValueTable("scope", "kvtable1").join());

        when(this.mockControllerService.deleteKeyValueTable(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.FAILURE).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteKeyValueTable("scope", "kvtable2").join(),
                ex -> ex instanceof ControllerFailureException);

        when(this.mockControllerService.deleteKeyValueTable(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatus(Controller.DeleteKVTableStatus.Status.TABLE_NOT_FOUND).build()));
        Assert.assertFalse(this.testController.deleteKeyValueTable("scope", "kvtable3").join());

        when(this.mockControllerService.deleteKeyValueTable(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Controller.DeleteKVTableStatus.newBuilder()
                        .setStatusValue(-1).build()));
        assertThrows("Expected ControllerFailureException",
                () -> this.testController.deleteKeyValueTable("scope", "kvtable4").join(),
                ex -> ex instanceof ControllerFailureException);
    }

=======
    public void testKeyValueTables() {
        AssertExtensions.assertThrows(
                "",
                () -> this.testController.createKeyValueTable("", "", null),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> this.testController.deleteKeyValueTable("", ""),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> this.testController.getCurrentSegmentsForKeyValueTable("", ""),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> this.testController.listKeyValueTables(""),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "",
                () -> this.testController.updateKeyValueTable("", "", null),
                ex -> ex instanceof UnsupportedOperationException);
    }
>>>>>>> Issue 4603: (KeyValueTables) Client Controller API (#4612)
}
