package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.tables.State;
import lombok.Data;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class TaskHelper {
    public static <T> CompletableFuture<VersionedMetadataAndState<T>> resetStateConditionally(StreamMetadataStore streamMetadataStore,
                                   String scope, String stream, Supplier<CompletableFuture<VersionedMetadata<T>>> versionedRecordSupplier,
                                   Predicate<T> started, State desiredState, String errorMessage, OperationContext context,
                                   ScheduledExecutorService executor) {
        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> versionedRecordSupplier.get()
                        .thenCompose(versionedMetadata -> {
                            if (started.test(versionedMetadata.getObject())) {
                                CompletableFuture<Integer> future = CompletableFuture.completedFuture(null);
                                if (versionedState.getObject().equals(desiredState)) {
                                    future = streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                            versionedState.getVersion(), context, executor);
                                }

                                // TODO shivesh: we dont always want to throw task start exception.
                                return future.thenApply(x -> {
                                    throw new TaskExceptions.StartException(errorMessage);
                                });
                            } else {
                                return CompletableFuture.completedFuture(new VersionedMetadataAndState<>(versionedMetadata, versionedState));
                            }
                        }));
    }

    @Data
    public static class VersionedMetadataAndState<T> {
        private final VersionedMetadata<T> metadata;
        private final VersionedMetadata<State> state;
    }
}
