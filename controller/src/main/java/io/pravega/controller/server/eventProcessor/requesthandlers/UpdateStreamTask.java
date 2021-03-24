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
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Request handler for performing scale operations received from requeststream.
 */
@Slf4j
public class UpdateStreamTask implements StreamTask<UpdateStreamEvent> {

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;

    public UpdateStreamTask(final StreamMetadataTasks streamMetadataTasks,
                            final StreamMetadataStore streamMetadataStore,
                            final BucketStore bucketStore, 
                            final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(bucketStore);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final UpdateStreamEvent request) {
        final OperationContext context = streamMetadataStore.createContext(request.getScope(), request.getStream());

        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                        .thenCompose(versionedMetadata -> {
                            if (!versionedMetadata.getObject().isUpdating()) {
                                if (versionedState.getObject().equals(State.UPDATING)) {
                                    return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                            versionedState, context, executor));
                                } else {
                                    return CompletableFuture.completedFuture(null);
                                }
                            } else {
                                return processUpdate(scope, stream, versionedMetadata, versionedState, context, requestId);
                            }
                        }));
    }

    private CompletableFuture<Void> processUpdate(String scope, String stream, VersionedMetadata<StreamConfigurationRecord> record,
                                                  VersionedMetadata<State> state, OperationContext context, long requestId) {
        StreamConfigurationRecord configProperty = record.getObject();

        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.UPDATING, state, context, executor)
                .thenCompose(updated -> updateStreamForAutoStreamCut(scope, stream, configProperty, updated)
                        .thenCompose(x -> notifyPolicyUpdate(context, scope, stream, configProperty.getStreamConfiguration(), requestId))
                        .thenCompose(x -> streamMetadataStore.completeUpdateConfiguration(scope, stream, record, context, executor))
                        .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updated, context, executor))));
    }

    private CompletableFuture<Void> updateStreamForAutoStreamCut(String scope, String stream,
                        StreamConfigurationRecord configProperty, VersionedMetadata<State> updated) {
        if (configProperty.getStreamConfiguration().getRetentionPolicy() != null) {
            return bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor);
        } else {
            return bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor);
        }
    }

    private CompletableFuture<Boolean> notifyPolicyUpdate(OperationContext context, String scope, String stream,
                                                          StreamConfiguration newConfig, long requestId) {
        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> streamMetadataTasks.notifyPolicyUpdates(scope, stream, activeSegments,
                        newConfig.getScalingPolicy(), this.streamMetadataTasks.retrieveDelegationToken(), requestId))
                .handle((res, ex) -> {
                    if (ex == null) {
                        return true;
                    } else {
                        throw new CompletionException(ex);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> writeBack(UpdateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(UpdateStreamEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.UPDATING));

    }
}
