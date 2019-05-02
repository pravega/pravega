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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Data;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WireCommandsTest {

    private final UUID uuid = UUID.randomUUID();
    private final String testString1 = "testString1";
    private final String testString2 = "testString2";
    private final ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5 });
    private final ByteBuf buf = Unpooled.wrappedBuffer(buffer);
    private final byte b = -1;
    private final int i = 1;
    private final int length = 18;
    private final long l = 7L;

    @Test
    public void testHello() throws IOException {
        testCommand(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
    }

    @Test
    public void testPadding() throws IOException {
        testCommand(new WireCommands.Padding(length));
    }

    @Test
    public void testSetupAppend() throws IOException {
        testCommand(new WireCommands.SetupAppend(l, uuid, testString1, ""));
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
        testCommand(new WireCommands.AppendBlockEnd(uuid, i, buf, i, i, l));
    }

    @Data
    private static final class ConditionalAppendV7 implements WireCommand, Request {
        final WireCommandType type = WireCommandType.CONDITIONAL_APPEND;
        final UUID writerId;
        final long eventNumber;
        final long expectedOffset;
        final Event event;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(expectedOffset);
            event.writeFields(out);
        }

        @Override
        public long getRequestId() {
            return eventNumber;
        }

        @Override
        public void process(RequestProcessor cp) {
            //Unreachable. This should be handled in AppendDecoder.
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testConditionalAppend() throws IOException {
        testCommand(new WireCommands.ConditionalAppend(uuid, l, l, new Event(buf), l));

        // Test that we are able to decode a message with a previous version.
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ConditionalAppendV7 commandV7 = new ConditionalAppendV7(uuid, l, l, new Event(buf));
        commandV7.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.ConditionalAppend(uuid, l, l, new Event(buf), -1));
    }

    @Test
    public void testInvalidConditionalAppend() throws IOException {
        WireCommands.ConditionalAppend cmd = new WireCommands.ConditionalAppend(uuid, l, l, new Event(buf), l);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        cmd.writeFields(new DataOutputStream(bout));
        byte[] bytes = bout.toByteArray();

        // Invalid length scenario.
        assertThrows("Read with invalid buffer length.",
                     () -> WireCommands.ConditionalAppend.readFrom(new ByteBufInputStream(wrappedBuffer(bytes)), 4),
                     t -> t instanceof InvalidMessageException);
        // Invalid buffer data.
        assertThrows("Read with invalid data.",
                     () -> WireCommands.ConditionalAppend.readFrom(new ByteBufInputStream(buf), buf.capacity()),
                     t -> t instanceof EOFException);
        assertThrows("Unsupported operation",
                     () -> cmd.process(mock(RequestProcessor.class)),
                     t -> t instanceof UnsupportedOperationException);
    }

    @Test
    public void testAuthTokenCheckFalied() throws IOException {
        testCommand(new WireCommands.AuthTokenCheckFailed(l, ""));
        AtomicReference<Boolean> authTokenCheckFailedCalled = new AtomicReference<>(false);
        ReplyProcessor rp = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                authTokenCheckFailedCalled.set(true);
            }
        };

        new WireCommands.AuthTokenCheckFailed(0, "").process(rp);
        assertTrue("Process should call the corresponding API", authTokenCheckFailedCalled.get());
    }

    /*
     * Test that we are able to decode the message of a previous version.
     * Specifically here, we create a data structure that corresponds to the
     * response to append data that does not include the last field (version 2)
     * and check that we are able to decode it correctly.
     */
    @Data
    public static final class DataAppendedV2 implements WireCommand {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final UUID writerId;
        final long eventNumber;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
        }
    }

    @Test
    public void testDataAppended() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataAppendedV2 commandV2 = new DataAppendedV2(uuid, l);
        commandV2.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.DataAppended(-1L, uuid, l, -1, l));

        // Test that we are able to encode and decode the current response
        // to append data correctly.
        testCommand(new WireCommands.DataAppended(l, uuid, l, Long.MIN_VALUE, -l));
    }

    /*
     * Test compatibility in WireCommands error messages between versions 5 and 6 (added serverStackTrace field).
     */
    @Data
    public static final class WrongHostV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.WRONG_HOST;
        final long requestId;
        final String segment;
        final String correctHost;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeUTF(correctHost);
        }

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilityWrongHostV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        WrongHostV5 commandV5 = new WrongHostV5(l, "", "");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.WrongHost(l, "", "", ""));
    }

    @Data
    public static final class SegmentIsSealedV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_IS_SEALED;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilitySegmentIsSealedV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        SegmentIsSealedV5 commandV5 = new SegmentIsSealedV5(l, "");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.SegmentIsSealed(l, "", "", -1L));
    }

    @Data
    public static final class SegmentIsTruncatedV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_IS_TRUNCATED;
        final long requestId;
        final String segment;
        final long startOffset;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
            out.writeLong(startOffset);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilitySegmentIsTruncatedV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        SegmentIsTruncatedV5 commandV5 = new SegmentIsTruncatedV5(l, "", 0);
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.SegmentIsTruncated(l, "", 0, "", -1L));
    }

    @Data
    public static final class SegmentAlreadyExistsV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.SEGMENT_ALREADY_EXISTS;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        @Override
        public String toString() {
            return "Segment already exists: " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilitySegmentAlreadyExistsV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        SegmentAlreadyExistsV5 commandV5 = new SegmentAlreadyExistsV5(l, "segment");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.SegmentAlreadyExists(l, "segment",  ""));
    }

    @Data
    public static final class NoSuchSegmentV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.NO_SUCH_SEGMENT;
        final long requestId;
        final String segment;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(segment);
        }

        @Override
        public String toString() {
            return "No such segment: " + segment;
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilityNoSuchSegmentV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        NoSuchSegmentV5 commandV5 = new NoSuchSegmentV5(l, "");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.NoSuchSegment(l, "", "", -1));
    }

    @Data
    public static final class InvalidEventNumberV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.INVALID_EVENT_NUMBER;
        final UUID writerId;
        final long eventNumber;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
        }

        @Override
        public String toString() {
            return "Invalid event number: " + eventNumber + " for writer: " + writerId;
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public long getRequestId() {
            return eventNumber;
        }
    }

    @Test
    public void testCompatibilityInvalidEventNumberV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        InvalidEventNumberV5 commandV5 = new InvalidEventNumberV5(uuid, i);
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.InvalidEventNumber(uuid, i, ""));
    }

    @Data
    public static final class OperationUnsupportedV5 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.OPERATION_UNSUPPORTED;
        final long requestId;
        final String operationName;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(operationName);
        }

        @Override
        public boolean isFailure() {
            return true;
        }
    }

    @Test
    public void testCompatibilityOperationUnsupportedV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        OperationUnsupportedV5 commandV5 = new OperationUnsupportedV5(l, testString1);
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.OperationUnsupported(l, testString1, ""));
    }

    @Data
    public static final class AuthTokenCheckFailedV5 implements WireCommand {
        final WireCommandType type = WireCommandType.AUTH_TOKEN_CHECK_FAILED;
        final long requestId;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
        }
    }

    @Test
    public void testCompatibilityAuthTokenCheckFailedV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        AuthTokenCheckFailedV5 commandV5 = new AuthTokenCheckFailedV5(l);
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.AuthTokenCheckFailed(l, ""));
    }

    @Test
    public void testConditionalCheckFailed() throws IOException {
        testCommand(new WireCommands.ConditionalCheckFailed(uuid, l, l));
    }

    @Test
    public void testReadSegment() throws IOException {
        testCommand(new WireCommands.ReadSegment(testString1, l, i, "", l));
    }

    @Test
    public void testSegmentRead() throws IOException {
        testCommand(new WireCommands.SegmentRead(testString1, l, true, false, buffer, l));
    }
    
    @Test
    public void testUpdateSegmentAttribute() throws IOException {
        testCommand(new WireCommands.UpdateSegmentAttribute(l, testString1, uuid, l, l, ""));
    }
    
    @Test
    public void testSegmentAttributeUpdated() throws IOException {
        testCommand(new WireCommands.SegmentAttributeUpdated(l, true));
        testCommand(new WireCommands.SegmentAttributeUpdated(l, false));
    }

    @Test
    public void testGetSegmentAttribute() throws IOException {
        testCommand(new WireCommands.GetSegmentAttribute(l, testString1, uuid, ""));
    }
    
    @Test
    public void testSegmentAttribute() throws IOException {
        testCommand(new WireCommands.SegmentAttribute(l, l + 1));
    }
    
    @Test
    public void testGetStreamSegmentInfo() throws IOException {
        testCommand(new WireCommands.GetStreamSegmentInfo(l, testString1, ""));
    }

    @Test
    public void testStreamSegmentInfo() throws IOException {
        testCommand(new WireCommands.StreamSegmentInfo(l - 1, testString1, true, false, false, l, l + 1, l - 1));
    }

    @Test
    public void testCreateSegment() throws IOException {
        testCommand(new WireCommands.CreateSegment(l, testString1, b, i, ""));
    }

    @Test
    public void testCreateTableSegment() throws IOException {
        testCommand(new WireCommands.CreateTableSegment(l, testString1, ""));
    }

    @Test
    public void testSegmentCreated() throws IOException {
        testCommand(new WireCommands.SegmentCreated(l, testString1));
    }

    @Test
    public void testMergeSegments() throws IOException {
        testCommand(new WireCommands.MergeSegments(l, testString1, testString2, ""));
    }

    @Test
    public void testMergeTableSegments() throws IOException {
        testCommand(new WireCommands.MergeTableSegments(l, testString1, testString2, ""));
    }

    @Test
    public void testSegmentsMerged() throws IOException {
        testCommand(new WireCommands.SegmentsMerged(l, testString1, testString2, -l));
    }

    @Test
    public void testSealSegment() throws IOException {
        testCommand(new WireCommands.SealSegment(l, testString1, ""));
    }

    @Test
    public void testSealTableSegment() throws IOException {
        testCommand(new WireCommands.SealTableSegment(l, testString1, ""));
    }

    @Test
    public void testSegmentSealed() throws IOException {
        testCommand(new WireCommands.SegmentSealed(l, testString1));
    }

    @Test
    public void testTruncateSegment() throws IOException {
        testCommand(new WireCommands.TruncateSegment(l, testString1, l + 1, ""));
    }

    @Test
    public void testSegmentTruncated() throws IOException {
        testCommand(new WireCommands.SegmentTruncated(l, testString1));
    }

    @Test
    public void testSegmentIsTruncated() throws IOException {
        testCommand(new WireCommands.SegmentIsTruncated(l, testString1, l + 1, "SomeException", l));
    }

    @Test
    public void testDeleteSegment() throws IOException {
        testCommand(new WireCommands.DeleteSegment(l, testString1, ""));
    }

    @Test
    public void testDeleteTableSegment() throws IOException {
        testCommand(new WireCommands.DeleteTableSegment(l, testString1, true, ""));
        testCommand(new WireCommands.DeleteTableSegment(l, testString1, false, ""));
    }

    @Test
    public void testSegmentDeleted() throws IOException {
        testCommand(new WireCommands.SegmentDeleted(l, testString1));
    }

    @Test
    public void testUpdateSegmentPolicy() throws IOException {
        testCommand(new WireCommands.UpdateSegmentPolicy(l, testString1, b, i, ""));
    }

    @Test
    public void testSegmentPolicyUpdated() throws IOException {
        testCommand(new WireCommands.SegmentPolicyUpdated(l, testString1));
    }

    @Test
    public void testWrongHost() throws IOException {
        testCommand(new WireCommands.WrongHost(l, "Foo", testString1, "SomeException"));
    }

    @Test
    public void testSegmentIsSealed() throws IOException {
        testCommand(new WireCommands.SegmentIsSealed(l, testString1, "SomeException", l));
    }

    @Test
    public void testSegmentAlreadyExists() throws IOException {
        testCommand(new WireCommands.SegmentAlreadyExists(l, testString1, "SomeException"));
    }

    @Test
    public void testNoSuchSegment() throws IOException {
        testCommand(new WireCommands.NoSuchSegment(l, testString1, "SomeException", l));
    }

    @Test
    public void testNotEmptyTableSegment() throws IOException {
        WireCommands.TableSegmentNotEmpty cmd = new WireCommands.TableSegmentNotEmpty(l, testString1, "SomeException");
        testCommand(cmd);
        assertTrue(cmd.isFailure());
    }

    @Test
    public void testInvalidEventNumber() throws IOException {
        testCommand(new WireCommands.InvalidEventNumber(uuid, i, "SomeException"));
    }

    @Test
    public void testKeepAlive() throws IOException {
        testCommand(new WireCommands.KeepAlive());
    }

    @Test
    public void testUpdateTableEntries() throws IOException {
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = Arrays.asList(
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(WireCommands.TableKey.EMPTY, WireCommands.TableValue.EMPTY),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), WireCommands.TableValue.EMPTY));
        testCommand(new WireCommands.UpdateTableEntries(l, testString1, "", new WireCommands.TableEntries(entries)));
    }

    @Test
    public void testTableEntriesUpdated() throws IOException {
        testCommand(new WireCommands.TableEntriesUpdated(l, Arrays.asList(1L, 2L, 3L)));
    }

    @Test
    public void testRemoveTableKeys() throws IOException {
        testCommand(new WireCommands.RemoveTableKeys(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                                                                                       new WireCommands.TableKey(buf, 2L))));
    }

    @Test
    public void testTableKeysRemoved() throws IOException {
        testCommand(new WireCommands.TableKeysRemoved(l, testString1));
    }

    @Test
    public void testReadTable() throws IOException {
        testCommand(new WireCommands.ReadTable(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                                                                                 new WireCommands.TableKey(buf, 2L))));
    }

    @Test
    public void testTableRead() throws IOException {
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = Arrays.asList(
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, 1L), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, 2L), new WireCommands.TableValue(buf))
        );

        testCommand(new WireCommands.TableRead(l, testString1, new WireCommands.TableEntries(entries)));
    }

    @Test
    public void testKeyDoesNotExist() throws IOException {
        WireCommands.TableKeyDoesNotExist cmd = new WireCommands.TableKeyDoesNotExist(l, testString1, "");
        testCommand(cmd);
        assertTrue(cmd.isFailure());
    }

    @Test
    public void testKeyBadVersion() throws IOException {
        WireCommands.TableKeyBadVersion cmd = new WireCommands.TableKeyBadVersion(l, testString1, "");
        testCommand(cmd);
        assertTrue(cmd.isFailure());
    }

    @Test
    public void testGetTableKeys() throws IOException {
        WireCommands.ReadTableKeys cmd = new WireCommands.ReadTableKeys(l, testString1, "", 100, buf);
        testCommand(cmd);
        cmd = new WireCommands.ReadTableKeys(l, testString1, "", 100, wrappedBuffer(new byte[0]));
        testCommand(cmd);
    }

    @Test
    public void testGetTableEntries() throws IOException {
        WireCommands.ReadTableEntries cmd = new WireCommands.ReadTableEntries(l, testString1, "", 10, buf);
        testCommand(cmd);
        cmd = new WireCommands.ReadTableEntries(l, testString1, "", 10, wrappedBuffer(new byte[0]));
        testCommand(cmd);
    }

    @Test
    public void testTableKeysIteratorItem() throws IOException {
        List<WireCommands.TableKey> keys = Arrays.asList(new WireCommands.TableKey(buf, 1L), new WireCommands.TableKey(buf, 2L));
        WireCommands.TableKeysRead cmd = new WireCommands.TableKeysRead(l, testString1, keys, buf);
        testCommand(cmd);
        cmd = new WireCommands.TableKeysRead(l, testString1, keys, wrappedBuffer(new byte[0]));
        testCommand(cmd);
    }

    @Test
    public void testTableEntriesIteratorItem() throws IOException {

        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = Arrays.asList(
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), new WireCommands.TableValue(buf)));
        WireCommands.TableEntries tableEntries = new WireCommands.TableEntries(entries);

        WireCommands.TableEntriesRead cmd = new WireCommands.TableEntriesRead(l, testString1, tableEntries, buf);
        testCommand(cmd);
        cmd = new WireCommands.TableEntriesRead(l, testString1, tableEntries, wrappedBuffer(new byte[0]));
        testCommand(cmd);
    }



    private void testCommand(WireCommand command) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        command.writeFields(new DataOutputStream(bout));
        byte[] array = bout.toByteArray();
        WireCommand read = command.getType().readFrom(new ByteBufInputStream(Unpooled.wrappedBuffer(array)),
                                                      array.length);
        assertEquals(command, read);
    }

    private void testCommandFromByteArray(byte[] bytes, WireCommand compatibleCommand) throws IOException {
        WireCommand read = compatibleCommand.getType().readFrom(new ByteBufInputStream(Unpooled.wrappedBuffer(bytes)),
                bytes.length);
        assertEquals(compatibleCommand, read);
    }

}
