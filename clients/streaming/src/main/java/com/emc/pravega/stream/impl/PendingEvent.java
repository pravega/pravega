/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Serializer;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import lombok.Data;

/**
 * This is a internal wrapper object used in the writer to pass along the routing key and the future
 * with the actual event during write.
 */
@Data
public class PendingEvent {
    public static final int MAX_WRITE_SIZE = Serializer.MAX_EVENT_SIZE;
    /**
     * The routing key that was provided to route the data.
     */
    private final String routingKey;
    /**
     * The data to be written. Note this is limited to {@value #MAX_WRITE_SIZE} bytes.
     */
    private final ByteBuffer data;
    /**
     * Callback to be invoked when the data is written.
     */
    private final CompletableFuture<Boolean> ackFuture;
    /**
     * If this is not null the data should only be written if the segment is of this length before the data is added.
     */
    private final Long expectedOffset;
    
    public PendingEvent(String routingKey, ByteBuffer data, CompletableFuture<Boolean> ackFuture) {
        this(routingKey, data, ackFuture, null);
    }
    
    public PendingEvent(String routingKey, ByteBuffer data, CompletableFuture<Boolean> ackFuture, Long expectedOffset) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(ackFuture);
        Preconditions.checkArgument(data.remaining() <= MAX_WRITE_SIZE, "Write size too large: %s", data.remaining());
        this.routingKey = routingKey;
        this.data = data;
        this.ackFuture = ackFuture;
        this.expectedOffset = expectedOffset;
    }
}
