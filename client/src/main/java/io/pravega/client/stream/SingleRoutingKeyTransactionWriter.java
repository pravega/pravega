/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

/**
 * A single routing key transaction writer that is similar to {@link TransactionalEventStreamWriter}, however it imposes constraints
 * on size of payload and routing keys. 
 * This writer uses the {@link SingleRoutingKeyTransaction} to write multiple events with same routing key atomically into a pravega stream. 
 * 
 * Prior to committing a transaction, the events written to it cannot be read or otherwise seen by readers.
 */
public interface SingleRoutingKeyTransactionWriter<Type> extends AutoCloseable  {
    
    /**
     * Start a new transaction on this stream. This allows events written to the transaction be written an committed atomically.
     * Note that transactions can only be open for {@link EventWriterConfig#transactionTimeoutTime}.
     * 
     * @return A transaction through which multiple events can be written atomically.
     */
    SingleRoutingKeyTransaction<Type> beginTxn();
    
    /**
     * Closes the writer. (No further methods may be called)
     */
    @Override
    void close();
}
