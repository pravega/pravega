package io.pravega.client.tables;

import java.util.function.Consumer;

public interface KeyUpdateListener<KeyT, ValueT> extends AutoCloseable {
    KeyT getListeningKey();

    void handleValueUpdate(Consumer<GetResult<ValueT>> value);

    void handlevalueRemoved(Consumer<KeyVersion> removeVersion);

    @Override
    void close();
}
