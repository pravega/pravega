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
package io.pravega.client.stream;

import java.util.UUID;

/**
 * A writer that writes Events to an Event stream transactionally. All events that are written as
 * part of a transaction can be committed atomically by calling {@link Transaction#commit()}. This
 * will result in either all of those events going into the stream or none of them and the commit
 * call failing with an exception.
 * 
 * Prior to committing a transaction, the events written to it cannot be read or otherwise seen by
 * readers.
 */
public interface TransactionalEventStreamWriter<Type> extends AutoCloseable  {
    
    /**
     * Start a new transaction on this stream. This allows events written to the transaction be written an committed atomically.
     * Note that transactions can only be open for {@link EventWriterConfig#transactionTimeoutTime}.
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
     * Returns the configuration that this writer was created with.
     *
     * @return Writer configuration
     */
    EventWriterConfig getConfig();
    
    /**
     * Closes the writer. (No further methods may be called)
     */
    @Override
    void close();
}
