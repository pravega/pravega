package com.emc.nautilus.streaming;

import java.util.concurrent.Future;

public interface Producer<Type> {
    Future<Void> publish(String routingKey, Type event);

    Transaction<Type> startTransaction(long transactionTimeout);

    ProducerConfig getConfig();

    void flush();

    void close();
}
