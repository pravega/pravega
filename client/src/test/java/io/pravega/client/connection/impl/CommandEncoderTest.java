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
package io.pravega.client.connection.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlock;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CommandEncoderTest {
    
    @RequiredArgsConstructor
    private static class FixedBatchSizeTracker implements AppendBatchSizeTracker {
        private final int batchSize;
        
        @Override
        public void recordAppend(long eventNumber, int size) {
            //Do nothing.
        }

        @Override
        public long recordAck(long eventNumber) {
            return 0;
        }

        @Override
        public int getAppendBlockSize() {
            return batchSize;
        }

        @Override
        public int getBatchTimeout() {
            return 10000;
        }
    }

    private static class DecodingOutputStream extends OutputStream {

        final ArrayList<WireCommand> decoded = new ArrayList<>();
        final IoBuffer buffer = new IoBuffer();
        
        @Override
        public void write(int b) throws IOException {
            throw new IllegalStateException("Single byte write calls should never be used");
        }
        
        @Override
        public void write(byte[] buf, int offset, int length) throws IOException {
            ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
            while (stream.available() > 0 || buffer.getBuffer() != null) {
                WireCommand command = TcpClientConnection.ConnectionReader.readCommand(stream, buffer);
                decoded.add(command);
            }
        }
    }
    
    @Test
    public void testRoundTrip() throws IOException {
        AppendBatchSizeTrackerImpl batchSizeTracker = new AppendBatchSizeTrackerImpl();
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        WireCommand command = new WireCommands.Hello(0, 1);
        commandEncoder.write(command);
        assertEquals(output.decoded.remove(0), command);
        command = new WireCommands.CreateTableSegment(0, "segment", false, "");
        commandEncoder.write(command);
        assertEquals(output.decoded.remove(0), command);
        command = new WireCommands.TruncateSegment(12, "s", 354, "d");
        commandEncoder.write(command);
        assertEquals(output.decoded.remove(0), command);        
    }
    
    @Test
    public void testAppendsAreBatched() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId, 1, event, 0);
        Append append2 = new Append("seg", writerId, 2, event, 0);
        Append append3 = new Append("seg", writerId, 3, event, 0);
        commandEncoder.write(append1);
        commandEncoder.write(append2);
        commandEncoder.write(append3);
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId, blockEnd.getWriterId());
        assertEquals(80 + 16, blockEnd.getSizeOfWholeEvents());
        assertEquals(40 + 4, blockEnd.getData().readableBytes());
        assertEquals(3, blockEnd.getNumEvents());
    }
    
    @Test
    public void testExactBatch() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);
        Event event = new WireCommands.Event(data);
        Append append = new Append("seg", writerId, 1, event, 0);
        commandEncoder.write(append);
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId, blockEnd.getWriterId());
        assertEquals(108, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
    }
    
    @Test
    public void testOverBatchSize() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[200]);
        Event event = new WireCommands.Event(data);
        Append append = new Append("seg", writerId, 1, event, 0);
        commandEncoder.write(append);
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(208, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId, blockEnd.getWriterId());
        assertEquals(208, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
    }
    
    @Test
    public void testBatchInterupted() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId, 1, event, 0);
        commandEncoder.write(append1);
        
        commandEncoder.write(new WireCommands.KeepAlive());
        
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId, blockEnd.getWriterId());
        assertEquals(48, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
        WireCommands.KeepAlive breakingCommand = (WireCommands.KeepAlive) output.decoded.remove(0);
        assertNotNull(breakingCommand);
    }
    
    @Test
    public void testBatchTimeout() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId, 1, event, 0);
        commandEncoder.write(append1);
        
        long l = commandEncoder.batchTimeout(0);
        commandEncoder.batchTimeout(l); //Triggers a timeout
        
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId, blockEnd.getWriterId());
        assertEquals(48, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
        assertTrue(output.decoded.isEmpty());
    }
    
    @Test
    public void testAppendsQueued() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId1 = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId1, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        UUID writerId2 = UUID.randomUUID();
        setupAppend = new WireCommands.SetupAppend(0, writerId2, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId1, 1, event, 0);
        commandEncoder.write(append1);
        
        Append appendOther = new Append("seg", writerId2, 100, event, 0);
        commandEncoder.write(appendOther);

        Append append2 = new Append("seg", writerId1, 2, event, 0);
        Append append3 = new Append("seg", writerId1, 3, event, 0);
        commandEncoder.write(append2);
        commandEncoder.write(append3);
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId1, blockEnd.getWriterId());
        assertEquals(80 + 16, blockEnd.getSizeOfWholeEvents());
        assertEquals(40 + 4, blockEnd.getData().readableBytes());
        assertEquals(3, blockEnd.getNumEvents());
        
        block = (AppendBlock) output.decoded.remove(0);
        assertEquals(48, block.getData().readableBytes());
        blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId2, blockEnd.getWriterId());
        assertEquals(48, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
    }
    
    @Test
    public void testAppendsQueuedBreak() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId1 = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId1, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        UUID writerId2 = UUID.randomUUID();
        setupAppend = new WireCommands.SetupAppend(0, writerId2, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId1, 1, event, 0);
        commandEncoder.write(append1);
        
        Append appendOther1 = new Append("seg", writerId2, 101, event, 0);
        Append appendOther2 = new Append("seg", writerId2, 102, event, 0);
        Append appendOther3 = new Append("seg", writerId2, 103, event, 0);
        commandEncoder.write(appendOther1);        
        commandEncoder.write(appendOther2);
        commandEncoder.write(appendOther3);
        
        commandEncoder.write(new WireCommands.KeepAlive());
        
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId1, blockEnd.getWriterId());
        assertEquals(48, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
        
        block = (AppendBlock) output.decoded.remove(0);
        assertEquals(48 * 3, block.getData().readableBytes());
        blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId2, blockEnd.getWriterId());
        assertEquals(48 * 3, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(3, blockEnd.getNumEvents());
    }
    
    @Test
    public void testAppendSizeQueuedBreak() throws IOException {
        AppendBatchSizeTracker batchSizeTracker = new FixedBatchSizeTracker(100);
        DecodingOutputStream output = new DecodingOutputStream();
        CommandEncoder commandEncoder = new CommandEncoder(x -> batchSizeTracker, null, output);
        UUID writerId1 = UUID.randomUUID();
        WireCommand setupAppend = new WireCommands.SetupAppend(0, writerId1, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        UUID writerId2 = UUID.randomUUID();
        setupAppend = new WireCommands.SetupAppend(0, writerId2, "seg", "");
        commandEncoder.write(setupAppend);
        assertEquals(output.decoded.remove(0), setupAppend);
        
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        Append append1 = new Append("seg", writerId1, 1, event, 0);
        commandEncoder.write(append1);
        
        Append appendOther1 = new Append("seg", writerId2, 101, event, 0);
        WireCommands.Event largeEvent = new WireCommands.Event(Unpooled.wrappedBuffer(new byte[CommandEncoder.MAX_QUEUED_SIZE]));
        Append appendOther2 = new Append("seg", writerId2, 102, largeEvent, 0);
        commandEncoder.write(appendOther1);        
        commandEncoder.write(appendOther2);
        
        AppendBlock block = (AppendBlock) output.decoded.remove(0);
        assertEquals(108, block.getData().readableBytes());
        AppendBlockEnd blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId1, blockEnd.getWriterId());
        assertEquals(48, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(1, blockEnd.getNumEvents());
        
        block = (AppendBlock) output.decoded.remove(0);
        assertEquals(CommandEncoder.MAX_QUEUED_SIZE + 48 + 8, block.getData().readableBytes());
        blockEnd = (AppendBlockEnd) output.decoded.remove(0);
        assertEquals(writerId2, blockEnd.getWriterId());
        assertEquals(CommandEncoder.MAX_QUEUED_SIZE + 48 + 8, blockEnd.getSizeOfWholeEvents());
        assertEquals(0, blockEnd.getData().readableBytes());
        assertEquals(2, blockEnd.getNumEvents());
    }
    
    @Test
    public void testValidateAppend() {
        UUID writerId = UUID.randomUUID();
        ByteBuf data = Unpooled.wrappedBuffer(new byte[40]);
        WireCommands.Event event = new WireCommands.Event(data);
        assertThrows(InvalidMessageException.class, () -> CommandEncoder.validateAppend(new Append("", writerId, 1, event, 1), null));
        CommandEncoder.Session s = Mockito.mock(CommandEncoder.Session.class);
        Mockito.doReturn(writerId).when(s).getId();
        CommandEncoder.validateAppend(new Append("", writerId, 1, event, 1), s);
        assertThrows(InvalidMessageException.class, () -> CommandEncoder.validateAppend(new Append("", writerId, -1, event, 1), s));
        assertThrows(IllegalArgumentException.class, () -> CommandEncoder.validateAppend(new Append("", writerId, 1, event, 132, 1), s));
    }
    
}
