/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.stream.Serializer;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.Data;

/**
 * This is a internal wrapper object used in the writer to pass along the routing key and the future
 * with the actual event during write.
 */
@Data
public class PendingEvent {
    /**
     * The serialized event max size. Equals to max event payload size plus additional 8 bytes for the wire command code
	 * and the payload size.
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
    /**
     * Callback to be invoked when the data is written.
     */
    private final CompletableFuture<Void> ackFuture;
       
    private PendingEvent(String routingKey, ByteBuf data, CompletableFuture<Void> ackFuture) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(data.readableBytes() <= MAX_WRITE_SIZE, "Write size too large: %s", data.readableBytes());
        this.routingKey = routingKey;
        this.data = data;
        this.ackFuture = ackFuture;
    }
    
    public static PendingEvent withHeader(String routingKey, ByteBuffer data, CompletableFuture<Void> ackFuture) {
        ByteBuf eventBuf = new Event(Unpooled.wrappedBuffer(data)).getAsByteBuf();
        return new PendingEvent(routingKey, eventBuf, ackFuture);
        
    }
    
    public static PendingEvent withoutHeader(String routingKey, ByteBuffer data, CompletableFuture<Void> ackFuture) {
        return new PendingEvent(routingKey, Unpooled.wrappedBuffer(data), ackFuture);
    }
}
