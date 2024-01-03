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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.auth.InvalidTokenException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.InlineExecutor;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed.ErrorCode.TOKEN_CHECK_FAILED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PravegaRequestProcessorAuthFailedTest {

    private PravegaRequestProcessor processor;
    private ServerConnection connection;

    @Before
    public void setUp() throws Exception {
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        connection = mock(ServerConnection.class);
        @Cleanup("shutdown")
        ScheduledExecutorService executor = new InlineExecutor();
        processor = new PravegaRequestProcessor(store, mock(TableStore.class), new TrackedConnection(connection),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
                (resource, token, expectedLevel) -> {
                    throw new InvalidTokenException("Token verification failed.");
                }, false, new IndexAppendProcessor(executor, store));
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void readSegment() {
        processor.readSegment(new WireCommands.ReadSegment("segment", 0, 10, "", 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(0, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void updateSegmentAttribute() {
        processor.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(100L, "segment",
                null, 0, 0, "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void getSegmentAttribute() {
        processor.getSegmentAttribute(new WireCommands.GetSegmentAttribute(100L, "segment",
                null, "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void getStreamSegmentInfo() {
        processor.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(100L,
                "segment", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void createSegment() {
        processor.createSegment(new WireCommands.CreateSegment(100L, "segment", (byte) 0, 0, "token", 0));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void mergeSegments() {
        processor.mergeSegments(new WireCommands.MergeSegments(100L, "segment", "segment2", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void sealSegment() {
        processor.sealSegment(new WireCommands.SealSegment(100L, "segment", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void truncateSegment() {
        processor.truncateSegment(new WireCommands.TruncateSegment(100L, "segment", 0, "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void deleteSegment() {
        processor.deleteSegment(new WireCommands.DeleteSegment(100L, "segment", "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void updateSegmentPolicy() {
        processor.updateSegmentPolicy(new WireCommands.UpdateSegmentPolicy(100L, "segment", (byte) 0, 0, "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }

    @Test
    public void locateOffset() {
        processor.locateOffset(new WireCommands.LocateOffset(100L, "segment", (byte) 0, "token"));
        verify(connection).send(new WireCommands.AuthTokenCheckFailed(100L, "", TOKEN_CHECK_FAILED));
    }
}