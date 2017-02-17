/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import java.util.concurrent.CompletableFuture;

import lombok.Data;

/**
 * This is a internal wrapper object used in the writer to pass along the routing key and the future with the actual
 * event durring write.
 * 
 * @param <Type> The type of event the client is producing
 */
@Data
public class PendingEvent<Type> {
    private final Type value;
    private final String routingKey;
    private final CompletableFuture<Boolean> ackFuture;
}
