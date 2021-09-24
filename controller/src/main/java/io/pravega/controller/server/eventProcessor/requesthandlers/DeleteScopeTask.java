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

import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for executing a delete operation for a ReaderGroup.
 */
public class DeleteScopeTask implements ScopeTask<DeleteScopeEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(DeleteScopeTask.class));

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public DeleteScopeTask(final StreamMetadataTasks streamMetaTasks,
                                 final StreamMetadataStore streamMetaStore,
                                 final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(streamMetaTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetaStore;
        this.streamMetadataTasks = streamMetaTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final DeleteScopeEvent request) {
        String scope = request.getScope();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createScopeContext(scope, requestId);
        log.debug(requestId, "Deleting {} scope", scope);
        // event should have UUID
        // UUID of event should be matching with UUID of scope
        // if not do nothing
        // if yes then check entry if present in scopes table- StreamMetadataStore.checkScopeExits(scope)
        streamMetadataStore.checkScopeExists(scope, context, executor);
        streamMetadataStore.deleteScopeRecursive(scope, context, executor).thenCompose(i -> CompletableFuture.completedFuture(null));
        return CompletableFuture.completedFuture(null);
    }
}

