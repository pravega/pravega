package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;

import java.util.concurrent.CompletableFuture;

public class InMemoryReaderGroup extends AbstractReaderGroup {
    @Override
    CompletableFuture<Void> createMetadataTables() {
        return null;
    }

    @Override
    CompletableFuture<Void> storeCreationTimeIfAbsent(long creationTime) {
        return null;
    }

    @Override
    CompletableFuture<Void> createConfigurationIfAbsent(ReaderGroupConfig data) {
        return null;
    }

    @Override
    CompletableFuture<Void> createStateIfAbsent() {
        return null;
    }

    @Override
    public void refresh() {

    }
}
