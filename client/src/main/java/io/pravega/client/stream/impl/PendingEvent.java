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
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.stream.Serializer;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.Data;
import lombok.NonNull;

/**
 * This is a internal wrapper object used in the writer to pass along the routing key and the future
 * with the actual event during write.
 */
@Data
public class PendingEvent {
    /**
     * The serialized event max size. Equals to the max event payload size plus additional 8 bytes for the wire command
     * code and the payload size.
     * @see Event for the details.
     */
    public static final int MAX_WRITE_SIZE = Serializer.MAX_EVENT_SIZE + 8;
    /**
     * The routing key that was provided to route the data.
     */
    private final String routingKey;
    /**
     * The data to be written. Note this is limited to {@value #MAX_WRITE_SIZE} bytes.
     */
    private final ByteBuf data;
    
    private final int eventCount;
    /**
     * Callback to be invoked when the data is written.
     */
    private final CompletableFuture<Void> ackFuture;
       
    private PendingEvent(String routingKey, ByteBuf data, int eventCount, CompletableFuture<Void> ackFuture) {
        Preconditions.checkNotNull(data);
        this.routingKey = routingKey;
        this.data = data;
        this.ackFuture = ackFuture;
        this.eventCount = eventCount;
    }
    
    public static PendingEvent withHeader(String routingKey, ByteBuffer data, CompletableFuture<Void> ackFuture) {
        ByteBuf eventBuf = getByteBuf(data);
        return new PendingEvent(routingKey, eventBuf, 1, ackFuture);
    }
    
    public static PendingEvent withHeader(@NonNull String routingKey, @NonNull List<ByteBuffer> batch, @NonNull CompletableFuture<Void> ackFuture) {
        Preconditions.checkArgument(!batch.isEmpty(), "Batch cannot be empty");
        ByteBuf[] buffers = new ByteBuf[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            buffers[i] = getByteBuf(batch.get(i));
        }

        ByteBuf batchBuff = Unpooled.wrappedUnmodifiableBuffer(buffers);
        Preconditions.checkArgument(batchBuff.readableBytes() <= 2 * MAX_WRITE_SIZE, "Batch size too large: %s", batchBuff.readableBytes());

        return new PendingEvent(routingKey, batchBuff, batch.size(), ackFuture);
    }

    public static PendingEvent withoutHeader(String routingKey, ByteBuffer data, CompletableFuture<Void> ackFuture) {
        ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
        Preconditions.checkArgument(dataBuf.readableBytes() <= MAX_WRITE_SIZE, "Write size too large: %s", dataBuf.readableBytes());

        return new PendingEvent(routingKey, dataBuf, 1, ackFuture);
    }

    private static ByteBuf getByteBuf(ByteBuffer data) {
        ByteBuf eventBuf = new Event(Unpooled.wrappedBuffer(data)).getAsByteBuf();
        Preconditions.checkArgument(eventBuf.readableBytes() <= MAX_WRITE_SIZE, "Write size too large: %s", eventBuf.readableBytes());
        return eventBuf;
    }
}
