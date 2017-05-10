/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WireCommandsTest {

    private final UUID uuid = UUID.randomUUID();
    private final String testString1 = "testString1";
    private final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 });
    private final ByteBuf buf = Unpooled.wrappedBuffer(buffer);
    private final byte b = -1;
    private final int i = 1;
    private final int length = 18;
    private final long l = 7L;

    @Test
    public void testHello() throws IOException {
        testCommand(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATABLE_VERSION));
    }
    
    @Test
    public void testPadding() throws IOException {
        testCommand(new WireCommands.Padding(length));
    }

    @Test
    public void testSetupAppend() throws IOException {
        testCommand(new WireCommands.SetupAppend(l, uuid, testString1));
    }

    @Test
    public void testAppendSetup() throws IOException {
        testCommand(new WireCommands.AppendSetup(l, testString1, uuid, l));
    }

    @Test
    public void testAppendBlock() throws IOException {
        testCommand(new WireCommands.AppendBlock(uuid));
    }

    @Test
    public void testAppendBlockEnd() throws IOException {
        testCommand(new WireCommands.AppendBlockEnd(uuid, l, i, buf));
    }

    @Test
    public void testConditionalAppend() throws IOException {
        testCommand(new WireCommands.ConditionalAppend(uuid, l, l, buf));
    }

    @Test
    public void testDataAppended() throws IOException {
        testCommand(new WireCommands.DataAppended(uuid, l));
    }

    @Test
    public void testConditionalCheckFailed() throws IOException {
        testCommand(new WireCommands.ConditionalCheckFailed(uuid, l));
    }

    @Test
    public void testReadSegment() throws IOException {
        testCommand(new WireCommands.ReadSegment(testString1, l, i));
    }

    @Test
    public void testSegmentRead() throws IOException {
        testCommand(new WireCommands.SegmentRead(testString1, l, true, false, buffer));
    }

    @Test
    public void testGetStreamSegmentInfo() throws IOException {
        testCommand(new WireCommands.GetStreamSegmentInfo(l, testString1));
    }

    @Test
    public void testStreamSegmentInfo() throws IOException {
        testCommand(new WireCommands.StreamSegmentInfo(l - 1, testString1, true, false, false, l, l + 1));
    }

    @Test
    public void testGetTransactionInfo() throws IOException {
        testCommand(new WireCommands.GetTransactionInfo(l - 1, testString1, uuid));
    }

    @Test
    public void testTransactionInfo() throws IOException {
        testCommand(new WireCommands.TransactionInfo(l - 1, testString1, uuid, testString1, false, true, l, l + 1));
    }

    @Test
    public void testCreateSegment() throws IOException {
        testCommand(new WireCommands.CreateSegment(l, testString1, b, i));
    }

    @Test
    public void testSegmentCreated() throws IOException {
        testCommand(new WireCommands.SegmentCreated(l, testString1));
    }

    @Test
    public void testCreateTransaction() throws IOException {
        testCommand(new WireCommands.CreateTransaction(l, testString1, uuid));
    }

    @Test
    public void testTransactionCreated() throws IOException {
        testCommand(new WireCommands.TransactionCreated(l, testString1, uuid));
    }

    @Test
    public void testCommitTransaction() throws IOException {
        testCommand(new WireCommands.CommitTransaction(l, testString1, uuid));
    }

    @Test
    public void testTransactionCommitted() throws IOException {
        testCommand(new WireCommands.TransactionCommitted(l, testString1, uuid));
    }

    @Test
    public void testAbortTransaction() throws IOException {
        testCommand(new WireCommands.AbortTransaction(l, testString1, uuid));
    }

    @Test
    public void testTransactionAborted() throws IOException {
        testCommand(new WireCommands.TransactionAborted(l, testString1, uuid));
    }

    @Test
    public void testSealSegment() throws IOException {
        testCommand(new WireCommands.SealSegment(l, testString1));
    }

    @Test
    public void testSegmentSealed() throws IOException {
        testCommand(new WireCommands.SegmentSealed(l, testString1));
    }

    @Test
    public void testDeleteSegment() throws IOException {
        testCommand(new WireCommands.DeleteSegment(l, testString1));
    }

    @Test
    public void testSegmentDeleted() throws IOException {
        testCommand(new WireCommands.SegmentDeleted(l, testString1));
    }

    @Test
    public void testUpdateSegmentPolicy() throws IOException {
        testCommand(new WireCommands.UpdateSegmentPolicy(l, testString1, b, i));
    }

    @Test
    public void testSegmentPolicyUpdated() throws IOException {
        testCommand(new WireCommands.SegmentPolicyUpdated(l, testString1));
    }

    @Test
    public void testWrongHost() throws IOException {
        testCommand(new WireCommands.WrongHost(l, "Foo", testString1));
    }

    @Test
    public void testSegmentIsSealed() throws IOException {
        testCommand(new WireCommands.SegmentIsSealed(l, testString1));
    }

    @Test
    public void testSegmentAlreadyExists() throws IOException {
        testCommand(new WireCommands.SegmentAlreadyExists(l, testString1));
    }

    @Test
    public void testNoSuchSegment() throws IOException {
        testCommand(new WireCommands.NoSuchSegment(l, testString1));
    }

    @Test
    public void testNoSuchTransaction() throws IOException {
        testCommand(new WireCommands.NoSuchTransaction(l, testString1));
    }

    @Test
    public void testKeepAlive() throws IOException {
        testCommand(new WireCommands.KeepAlive());
    }

    private void testCommand(WireCommand command) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        command.writeFields(new DataOutputStream(bout));
        byte[] array = bout.toByteArray();
        WireCommand read = command.getType().readFrom(new DataInputStream(new ByteArrayInputStream(array)),
                                                      array.length);
        assertEquals(command, read);
    }

}
