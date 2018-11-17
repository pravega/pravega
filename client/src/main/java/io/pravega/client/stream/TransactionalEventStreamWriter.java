/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.util.UUID;

public interface TransactionalEventStreamWriter<Type> extends AutoCloseable  {
    
    /**
     * Start a new transaction on this stream. This allows events written to the transaction be written an committed atomically.
     * Note that transactions can only be open for {@link EventWriterConfig#getTransactionTimeoutTime()}.
     * 
     * @return A transaction through which multiple events can be written atomically.
     */
    Transaction<Type> beginTxn();

    /**
     * Returns a previously created transaction.
     * 
     * @param transactionId The result retained from calling {@link Transaction#getTxnId()}
     * @return Transaction object with given UUID
     */
    Transaction<Type> getTxn(UUID transactionId);

    /**
     * Returns the configuration that this writer was create with.
     *
     * @return Writer configuration
     */
    EventWriterConfig getConfig();
    
}
