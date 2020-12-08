package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;

import java.util.concurrent.CompletableFuture;

public class InMemoryReaderGroup extends AbstractReaderGroup {
    InMemoryReaderGroup(String scopeName, String rgName) {
        super(scopeName, rgName);
    }

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
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getConfigurationData(boolean ignoreCached) {
        return null;
    }

    @Override
    public void refresh() {

    }
}
