package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;

import java.util.concurrent.CompletableFuture;

public abstract class AbstractReaderGroup implements ReaderGroup {

    @Override
    public String getScope() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getScopeName() {
        return null;
    }

    @Override
    public CompletableFuture<Void> create(ReaderGroupConfig configuration, long createTimestamp) {
        return createMetadataTables()
                  .thenCompose((Void v) -> storeCreationTimeIfAbsent(createTimestamp))
                  .thenCompose((Void v) -> createStateIfAbsent())
                  .thenCompose((Void v) -> createConfigurationIfAbsent(configuration));
    }

    @Override
    public CompletableFuture<Void> delete() {
        return null;
    }

    @Override
    public CompletableFuture<Long> getCreationTime() {
        return null;
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(ReaderGroupConfig configuration) {
        return null;
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfig> existing) {
        return null;
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> getConfiguration() {
        return null;
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getVersionedConfigurationRecord() {
        return null;
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedState() {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateState(ReaderGroupState state) {
        return null;
    }

    @Override
    public CompletableFuture<VersionedMetadata<ReaderGroupState>> updateVersionedState(VersionedMetadata<ReaderGroupState> state, ReaderGroupState newState) {
        return null;
    }

    @Override
    public CompletableFuture<ReaderGroupState> getState(boolean ignoreCached) {
        return null;
    }

    abstract CompletableFuture<Void> createMetadataTables();

    abstract CompletableFuture<Void> storeCreationTimeIfAbsent(final long creationTime);

    abstract CompletableFuture<Void> createConfigurationIfAbsent(final ReaderGroupConfig data);
    abstract CompletableFuture<Void> createStateIfAbsent();

}
