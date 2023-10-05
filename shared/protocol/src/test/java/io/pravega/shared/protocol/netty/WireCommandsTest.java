/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.protocol.netty;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Data;
import lombok.ToString;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WireCommandsTest extends LeakDetectorTestSuite {

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

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.AppendBlock(uuid, buf),
                WireCommands.AppendBlock::readFrom,
                ab -> ab.getData().refCnt());
    }

    @Test
    public void testAppendBlockEnd() throws IOException {
        testCommand(new WireCommands.AppendBlockEnd(uuid, i, buf, i, i, l));

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.AppendBlockEnd(uuid, i, buf, i, i, l),
                WireCommands.AppendBlockEnd::readFrom,
                abe -> abe.getData().refCnt());
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

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.ConditionalAppend(uuid, l, l, new Event(buf), -1),
                WireCommands.ConditionalAppend::readFrom,
                ce -> ce.getEvent().getData().refCnt());
    }

    @Test
    public void testInvalidConditionalAppend() throws IOException {
        WireCommands.ConditionalAppend cmd = new WireCommands.ConditionalAppend(uuid, l, l, new Event(buf), l);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        cmd.writeFields(new DataOutputStream(bout));
        byte[] bytes = bout.toByteArray();

        // Invalid length scenario.
        assertThrows("Read with invalid buffer length.",
                () -> WireCommands.ConditionalAppend.readFrom(new EnhancedByteBufInputStream(wrappedBuffer(bytes)), 4),
                t -> t instanceof InvalidMessageException);
        // Invalid buffer data.
        assertThrows("Read with invalid data.",
                () -> WireCommands.ConditionalAppend.readFrom(new EnhancedByteBufInputStream(buf), buf.capacity()),
                t -> t instanceof EOFException);
        assertThrows("Unsupported operation",
                () -> cmd.process(mock(RequestProcessor.class)),
                t -> t instanceof UnsupportedOperationException);
    }

    @Test
    public void testPartialEvent() throws IOException {
        testCommand(new WireCommands.PartialEvent(buf));

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.PartialEvent(buf),
                WireCommands.PartialEvent::readFrom,
                pe -> pe.getData().refCnt());
    }

    @Test
    public void testAuthTokenCheckFailed() throws IOException {
        testCommand(new WireCommands.AuthTokenCheckFailed(l, "",
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED));
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

        new WireCommands.AuthTokenCheckFailed(0, "",
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED).process(rp);
        assertTrue("Process should call the corresponding API", authTokenCheckFailedCalled.get());

        assertFalse(new WireCommands.AuthTokenCheckFailed(0, "",
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED).isTokenExpired());

        assertTrue(new WireCommands.AuthTokenCheckFailed(0, "",
                WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_EXPIRED).isTokenExpired());
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
    
    @Data
    public static final class DataAppendedV3 implements WireCommand {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final UUID writerId;
        final long eventNumber;
        final long previousEventNumber;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(previousEventNumber);
        }
    }
    
    @Data
    public static final class DataAppendedV4 implements WireCommand {
        final WireCommandType type = WireCommandType.DATA_APPENDED;
        final long requestId;
        final UUID writerId;
        final long eventNumber;
        final long previousEventNumber;

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeLong(previousEventNumber);
            out.writeLong(requestId);
        }
    }

    @Test
    public void testDataAppended() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataAppendedV2 commandV2 = new DataAppendedV2(uuid, l);
        commandV2.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.DataAppended(-1L, uuid, l, -1, -1));

        bout = new ByteArrayOutputStream();
        DataAppendedV3 commandV3 = new DataAppendedV3(uuid, l, 2);
        commandV3.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.DataAppended(-1L, uuid, l, 2L, -1));
        
        bout = new ByteArrayOutputStream();
        DataAppendedV4 commandV4 = new DataAppendedV4(4, uuid, l, 3);
        commandV4.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.DataAppended(4L, uuid, l, 3L, -1));
        
        // Test that we are able to encode and decode the current response
        // to append data correctly.
        testCommand(new WireCommands.DataAppended(1, uuid, l, Long.MIN_VALUE, -l));
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
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.InvalidEventNumber(uuid, i, "", b));
    }

    @Data
    public static final class InvalidEventNumberV15 implements Reply, WireCommand {
        final WireCommandType type = WireCommandType.INVALID_EVENT_NUMBER;
        final UUID writerId;
        final long eventNumber;
        final String serverStackTrace;

        @Override
        public void process(ReplyProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(writerId.getMostSignificantBits());
            out.writeLong(writerId.getLeastSignificantBits());
            out.writeLong(eventNumber);
            out.writeUTF(serverStackTrace);
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
    public void testCompatibilityInvalidEventNumberV15() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        InvalidEventNumberV15 commandV5 = new InvalidEventNumberV15(uuid, i, "");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.InvalidEventNumber(uuid, i, "", b));
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
    public void testFlushToStorage() throws IOException {
        testCommand(new WireCommands.FlushToStorage(i, "", l));
    }

    @Test
    public void testStorageFlushed() throws IOException {
        testCommand(new WireCommands.StorageFlushed(l));
    }

    @Test
    public void testListStorageChunks() throws IOException {
        testCommand(new WireCommands.ListStorageChunks(testString1, "", l));
    }

    @Test
    public void testStorageChunksListed() throws IOException {
        List<WireCommands.ChunkInfo> chunks = Collections.singletonList(new WireCommands.ChunkInfo(l, l, l, testString1, false));
        testCommand(new WireCommands.StorageChunksListed(l, chunks));
    }

    @Test
    public void testReadSegment() throws IOException {
        testCommand(new WireCommands.ReadSegment(testString1, l, i, "", l));
    }

    @Test
    public void testSegmentRead() throws IOException {
        testCommand(new WireCommands.SegmentRead(testString1, l, true, false, buf, l));

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.SegmentRead(testString1, l, true, false, buf, l),
                WireCommands.SegmentRead::readFrom,
                sr -> sr.getData().refCnt());

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
        testCommand(new WireCommands.CreateSegment(l, testString1, b, i, "", 1024L));
    }

    @Test
    public void testGetTableSegmentInfo() throws IOException {
        testCommand(new WireCommands.GetTableSegmentInfo(l, testString1, ""));
    }

    @Test
    public void testTableSegmentInfo() throws IOException {
        testCommand(new WireCommands.TableSegmentInfo(l, testString1, l + 1, l + 2, 3, 4));
    }

    @Test
    public void testCreateTableSegment() throws IOException {
        testCommand(new WireCommands.CreateTableSegment(l, testString1, true, 16, "", 1024L));
    }

    @Test
    public void testSegmentCreated() throws IOException {
        testCommand(new WireCommands.SegmentCreated(l, testString1));
    }

    @Test
    public void testCreateTransientSegment() throws IOException {
        RequestProcessor rp = mock(RequestProcessor.class);
        WireCommands.CreateTransientSegment cmd = new WireCommands.CreateTransientSegment(l, new UUID(0, 0), testString1, "");
        cmd.process(rp);
        verify(rp, times(1)).createTransientSegment(cmd);
        testCommand(cmd);
    }

    @Test
    public void testMergeSegmentsBatch() throws IOException {
        List<String> txnSegmentIds = ImmutableList.of("txn1seg0", "txn2seg0");
        String streamSegmentId = "seg0";
        testCommand(new WireCommands.MergeSegmentsBatch(l, streamSegmentId, txnSegmentIds, ""));
    }

    @Test
    public void testSegmentsBatchMerged() throws IOException {
        List<String> txnSegmentIds = ImmutableList.of("txn1seg0", "txn2seg0");
        String streamSegmentId = "seg0";
        testCommand(new WireCommands.SegmentsBatchMerged(l, streamSegmentId, txnSegmentIds, ImmutableList.of(10L, 11L)));
    }

    @Test
    public void testMergeSegments() throws IOException {
        testCommand(new WireCommands.MergeSegments(l, testString1, testString2, ""));
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
        testCommand(new WireCommands.InvalidEventNumber(uuid, i, "SomeException", l));
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
        testCommand(new WireCommands.UpdateTableEntries(l, testString1, "", new WireCommands.TableEntries(entries), 0L));

        // Each Key and Value will retain the buffer once. We do not retain anything for the empty Table Key/Value.
        int refCntIncrement = entries.stream()
                .mapToInt(e -> (e.getKey() == WireCommands.TableKey.EMPTY ? 0 : 1) + (e.getValue() == WireCommands.TableValue.EMPTY ? 0 : 1))
                .sum();
        testReleasableCommand(
                () -> new WireCommands.UpdateTableEntries(l, testString1, "", new WireCommands.TableEntries(entries), 0L),
                WireCommands.UpdateTableEntries::readFrom,
                ce -> ce.tableEntries.getEntries().get(0).getValue().getData().refCnt(),
                refCntIncrement);
    }

    @Test
    public void testTableEntriesUpdated() throws IOException {
        testCommand(new WireCommands.TableEntriesUpdated(l, Arrays.asList(1L, 2L, 3L)));
    }

    @Test
    public void testRemoveTableKeys() throws IOException {
        testCommand(new WireCommands.RemoveTableKeys(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                new WireCommands.TableKey(buf, 2L)), 0L));
        testReleasableCommand(
                () -> new WireCommands.RemoveTableKeys(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                        new WireCommands.TableKey(buf, 2L)), 0L),
                WireCommands.RemoveTableKeys::readFrom,
                ce -> ce.getKeys().get(0).getData().refCnt(),
                2);
    }

    @Test
    public void testTableKeysRemoved() throws IOException {
        testCommand(new WireCommands.TableKeysRemoved(l, testString1));
    }

    @Test
    public void testLocateOffset() throws IOException {
        WireCommands.LocateOffset cmd = new WireCommands.LocateOffset(l, testString1, 10, "");
        testCommand(cmd);
    }

    @Test
    public void testOffsetLocated() throws IOException {
        WireCommands.OffsetLocated cmd = new WireCommands.OffsetLocated(l, testString1, 10);
        testCommand(cmd);
    }

    @Test
    public void testReadTable() throws IOException {
        testCommand(new WireCommands.ReadTable(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                new WireCommands.TableKey(buf, 2L))));
        testReleasableCommand(
                () -> new WireCommands.ReadTable(l, testString1, "", Arrays.asList(new WireCommands.TableKey(buf, 1L),
                        new WireCommands.TableKey(buf, 2L))),
                WireCommands.ReadTable::readFrom,
                ce -> ce.getKeys().get(0).getData().refCnt(),
                2);
    }

    @Test
    public void testTableRead() throws IOException {
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = Arrays.asList(
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, 1L), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, 2L), new WireCommands.TableValue(buf))
        );

        testCommand(new WireCommands.TableRead(l, testString1, new WireCommands.TableEntries(entries)));

        testReleasableCommand(
                () -> new WireCommands.TableRead(l, testString1, new WireCommands.TableEntries(entries)),
                WireCommands.TableRead::readFrom,
                ce -> ce.entries.getEntries().get(0).getKey().getData().refCnt(),
                4);
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
    public void testTableIterators() throws IOException {
        testTableIterators(args -> new WireCommands.ReadTableKeys(l, testString1, "", 100, args));
        testTableIterators(args -> new WireCommands.ReadTableEntries(l, testString1, "", 100, args));
    }

    private <T extends WireCommand> void testTableIterators(Function<WireCommands.TableIteratorArgs, T> createWireCommand) throws IOException {
        // Continuation Token.
        ByteBuf buf2 = buf.copy().setInt(0, Integer.MAX_VALUE);
        WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(buf, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        T cmd = createWireCommand.apply(args);
        testCommand(cmd);

        // From/To.
        args = new WireCommands.TableIteratorArgs(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, buf, buf2);
        cmd = createWireCommand.apply(args);
        testCommand(cmd);

        // Test that we are able to read fields from an older version.
        ByteBuf buf3 = buf.copy().setInt(0, Integer.MAX_VALUE - 1);
        args = new WireCommands.TableIteratorArgs(buf, Unpooled.EMPTY_BUFFER, buf2, buf3);
        cmd = createWireCommand.apply(args);
        ByteBufferOutputStream bout = new ByteBufferOutputStream();
        cmd.writeFields(new DataOutputStream(bout));
        T cmd2 = createWireCommand.apply(new WireCommands.TableIteratorArgs(buf, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER));
        testCommandFromByteArray(bout.getData().slice(0, bout.size() - 2 * Integer.BYTES - buf2.readableBytes() - buf3.readableBytes()).getCopy(), cmd2);
    }

    @Test
    public void testTableKeysIteratorItem() throws IOException {
        List<WireCommands.TableKey> keys = Arrays.asList(new WireCommands.TableKey(buf, 1L), new WireCommands.TableKey(buf, 2L));
        WireCommands.TableKeysRead cmd = new WireCommands.TableKeysRead(l, testString1, keys, buf);
        testCommand(cmd);
        cmd = new WireCommands.TableKeysRead(l, testString1, keys, wrappedBuffer(new byte[0]));
        testCommand(cmd);
        testReleasableCommand(
                () -> new WireCommands.TableKeysRead(l, testString1, keys, buf),
                WireCommands.TableKeysRead::readFrom,
                ce -> ce.keys.get(0).getData().refCnt(),
                keys.size());
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
        testReleasableCommand(
                () -> new WireCommands.TableEntriesRead(l, testString1, tableEntries, buf),
                WireCommands.TableEntriesRead::readFrom,
                ce -> ce.getEntries().getEntries().get(0).getKey().getData().refCnt(),
                2 * entries.size());
    }

    @Test
    public void testReadTableEntriesDelta() throws IOException {
        WireCommands.ReadTableEntriesDelta cmd = new WireCommands.ReadTableEntriesDelta(l, testString1, "", 1L, 100);
        testCommand(cmd);
    }

    @Test
    public void testTableEntriesDeltaRead() throws IOException {
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> entries = Arrays.asList(
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l), new WireCommands.TableValue(buf)),
                new SimpleImmutableEntry<>(new WireCommands.TableKey(buf, l + 1), new WireCommands.TableValue(buf)));
        WireCommands.TableEntries tableEntries = new WireCommands.TableEntries(entries);

        WireCommands.TableEntriesDeltaRead cmd = new WireCommands.TableEntriesDeltaRead(
                l, testString1, tableEntries, false, false, WireCommands.TableKey.NO_VERSION);
        testCommand(cmd);
    }

    @Test
    public void testConditionalBlockEnd() throws IOException {
        testCommand(new WireCommands.ConditionalBlockEnd(uuid, l, l, buf, l));

        // Test that it correctly implements ReleasableCommand.
        testReleasableCommand(
                () -> new WireCommands.ConditionalBlockEnd(uuid, l, l, buf, -1),
                WireCommands.ConditionalBlockEnd::readFrom,
                ce -> ce.getData().refCnt());
    }

    @Test
    public void testMergeSegmentsWithAttributes() throws IOException {
        List<WireCommands.ConditionalAttributeUpdate> attributeUpdates = Arrays.asList(
                new WireCommands.ConditionalAttributeUpdate(UUID.randomUUID(), WireCommands.ConditionalAttributeUpdate.REPLACE, 0, Long.MIN_VALUE),
                new WireCommands.ConditionalAttributeUpdate(UUID.randomUUID(), WireCommands.ConditionalAttributeUpdate.REPLACE_IF_EQUALS, 0, Long.MIN_VALUE));
        WireCommands.MergeSegments conditionalMergeSegments = new WireCommands.MergeSegments(l, testString1, testString2,
                "", attributeUpdates);
        testCommand(conditionalMergeSegments);
        // Check the size of the ConditionalAttributeUpdate.
        assertEquals(attributeUpdates.get(0).size(), 4 * Long.BYTES + 1);
    }

    @Data
    public static final class MergeSegmentsV5 implements Request, WireCommand {
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS;
        final long requestId;
        final String target;
        final String source;
        @ToString.Exclude
        final String delegationToken;

        public MergeSegmentsV5(long requestId, String target, String source, String delegationToken) {
            this.requestId = requestId;
            this.target = target;
            this.source = source;
            this.delegationToken = delegationToken;
        }

        @Override
        public void process(RequestProcessor cp) {}

        @Override
        public void writeFields(DataOutput out) throws IOException {
            out.writeLong(requestId);
            out.writeUTF(target);
            out.writeUTF(source);
            out.writeUTF(delegationToken == null ? "" : delegationToken);
        }
    }

    @Test
    public void testCompatibilityMergeSegmentsV5() throws IOException {
        // Test that we are able to decode a message with a previous version
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        MergeSegmentsV5 commandV5 = new MergeSegmentsV5(l, testString1, testString2, "");
        commandV5.writeFields(new DataOutputStream(bout));
        testCommandFromByteArray(bout.toByteArray(), new WireCommands.MergeSegments(l, testString1, testString2, "", Collections.emptyList()));
    }

    @Test
    public void testErrorMessage() throws IOException {
        for (WireCommands.ErrorMessage.ErrorCode code : WireCommands.ErrorMessage.ErrorCode.values()) {
            Class<? extends Throwable> exceptionType = code.getExceptionType();
            WireCommands.ErrorMessage cmd  = new WireCommands.ErrorMessage(1, "segment", testString1, code);
            testCommand(cmd);
            assertEquals(cmd.getErrorCode().getExceptionType(), exceptionType);
            assertEquals(WireCommands.ErrorMessage.ErrorCode.valueOf(exceptionType), code);

            RuntimeException exception = cmd.getThrowableException();
            AssertExtensions.assertThrows(exceptionType, () -> {
                throw exception;
            });
        }
    }

    private <T extends WireCommands.ReleasableCommand> void testReleasableCommand(
            Supplier<T> fromBuf, WireCommands.Constructor fromStream, Function<T, Integer> getRefCnt) throws IOException {
        testReleasableCommand(fromBuf, fromStream, getRefCnt, 1);
    }

    @SuppressWarnings("unchecked")
    private <T extends WireCommands.ReleasableCommand> void testReleasableCommand(
            Supplier<T> fromBuf, WireCommands.Constructor fromStream, Function<T, Integer> getRefCnt, int refCntIncrement) throws IOException {
        // If we pass in the buffer ourselves, there should be no need to release.
        final int originalRefCnt = buf.refCnt();
        int expectedRefCnt = originalRefCnt;
        T command = fromBuf.get();
        assertTrue(command.isReleased());
        command.release();
        assertEquals(originalRefCnt, buf.refCnt());
        assertTrue(command.isReleased());
        command.release(); // Do this again. The second time should have no effect.
        assertEquals(originalRefCnt, buf.refCnt());

        // Deserialize the command.
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        command.writeFields(new DataOutputStream(bout));
        ByteBuf buffer = Unpooled.wrappedBuffer(bout.toByteArray());
        T command2 = (T) fromStream.readFrom(new EnhancedByteBufInputStream(buffer), bout.size());
        expectedRefCnt += refCntIncrement;
        assertEquals(expectedRefCnt, (int) getRefCnt.apply(command2));
        assertEquals(expectedRefCnt, buffer.refCnt());

        // Release the underlying buffer.
        buffer.release();
        expectedRefCnt--;
        assertEquals(expectedRefCnt, (int) getRefCnt.apply(command2));
        assertEquals(expectedRefCnt, buffer.refCnt());

        // Release the command.
        command2.release();
        expectedRefCnt -= refCntIncrement;
        assertEquals(0, (int) getRefCnt.apply(command2));
        assertEquals(0, buffer.refCnt());
        command2.release(); // Do this again. The second time should have no effect.
        assertEquals(0, buffer.refCnt());
    }

    private void testCommand(WireCommand command) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        command.writeFields(new DataOutputStream(bout));
        byte[] array = bout.toByteArray();
        WireCommand read = command.getType().readFrom(new EnhancedByteBufInputStream(Unpooled.wrappedBuffer(array)),
                                                      array.length);
        assertEquals(command, read);
    }

    private void testCommandFromByteArray(byte[] bytes, WireCommand compatibleCommand) throws IOException {
        WireCommand read = compatibleCommand.getType().readFrom(new EnhancedByteBufInputStream(Unpooled.wrappedBuffer(bytes)),
                bytes.length);
        assertEquals(compatibleCommand, read);
    }

}
