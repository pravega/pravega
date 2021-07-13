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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommandDecoderTest {

    @Test
    public void testDecode() throws IOException {
        ByteBuf buffer = Unpooled.buffer(16);
        Hello command = new WireCommands.Hello(10, 1);     
        CommandEncoder.writeMessage(command, buffer);
        assertEquals(16, buffer.readableBytes());

        command = (Hello) CommandDecoder.parseCommand(buffer);
        assertEquals(0, buffer.readableBytes());
        assertEquals(WireCommandType.HELLO, command.getType());    
        assertEquals(10, command.highVersion); 
        assertEquals(1, command.lowVersion);    
    }

    @Test
    public void testDecodeLargeBuffer() throws IOException {
        ByteBuf buffer = Unpooled.buffer(100);
        Hello command = new WireCommands.Hello(10, 1);     
        CommandEncoder.writeMessage(command, buffer);
        buffer.writeLong(1); //Bonus data
        buffer.writeLong(2);
        buffer.writeLong(3);
        assertEquals(16 + 24, buffer.readableBytes());

        command = (Hello) CommandDecoder.parseCommand(buffer);
        assertEquals(24, buffer.readableBytes());
        assertEquals(WireCommandType.HELLO, command.getType());    
        assertEquals(10, command.highVersion); 
        assertEquals(1, command.lowVersion);          
    }
    
    @Test
    public void testUnknownFields() throws IOException {
        ByteBuf buffer = Unpooled.buffer(100);
        Hello command = new WireCommands.Hello(10, 1);     
        CommandEncoder.writeMessage(command, buffer);
        buffer.writeLong(1); //Bonus data
        buffer.writeLong(2);
        buffer.writeLong(3);
        buffer.setInt(4, 8 + 24);
        assertEquals(16 + 24, buffer.readableBytes());

        command = (Hello) CommandDecoder.parseCommand(buffer);
        assertEquals(0, buffer.readableBytes());
        assertEquals(WireCommandType.HELLO, command.getType());    
        assertEquals(10, command.highVersion); 
        assertEquals(1, command.lowVersion);          
    }
}
