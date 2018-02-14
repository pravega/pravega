/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PravegaRequestProcessorAuthFailedTest {

    private PravegaRequestProcessor processor;
    private ServerConnection connection;

    @Before
    public void setUp() throws Exception {
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        connection = mock(ServerConnection.class);
        processor = new PravegaRequestProcessor(store, connection, null, (resource, token, expectedLevel) -> false);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void readSegment() {
        processor.readSegment(new WireCommands.ReadSegment("segment", 0, "", 10));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(0));
    }

    @Test
    public void updateSegmentAttribute() {
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(100L, "segment",
                 "token", null, 0, 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void getSegmentAttribute() {
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(100L, "segment", "token",
                null));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void getStreamSegmentInfo() {
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(100L, "token",
                "segment"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void getTransactionInfo() {
        processor.getTransactionInfo(new WireCommands.GetTransactionInfo(100L, "segment", "token", UUID.randomUUID()));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void createSegment() {
        processor.createSegment(new WireCommands.CreateSegment(100L, "token", "segment", (byte) 0, 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void createTransaction() {
        processor.createTransaction(new WireCommands.CreateTransaction(100L, "token", "segment", UUID.randomUUID()));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void commitTransaction() {
        processor.commitTransaction(new WireCommands.CommitTransaction(100L, "segment", "token", UUID.randomUUID()));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void abortTransaction() {
        processor.abortTransaction(new WireCommands.AbortTransaction(100L, "token", "segment", UUID.randomUUID()));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void sealSegment() {
        processor.sealSegment(new WireCommands.SealSegment(100L, "token", "segment"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void truncateSegment() {
        processor.truncateSegment(new WireCommands.TruncateSegment(100L, "token", "segment", 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void deleteSegment() {
        processor.deleteSegment(new WireCommands.DeleteSegment(100L, "token", "segment"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }

    @Test
    public void updateSegmentPolicy() {
        processor.updateSegmentPolicy(new WireCommands.UpdateSegmentPolicy(100L, "segment", "token", (byte) 0, 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L));
    }
}