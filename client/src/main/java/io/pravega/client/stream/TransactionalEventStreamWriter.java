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
