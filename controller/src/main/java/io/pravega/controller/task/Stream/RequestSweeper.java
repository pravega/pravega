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
package io.pravega.controller.task.Stream;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.controller.util.RetryHelper.RETRYABLE_PREDICATE;
import static io.pravega.controller.util.RetryHelper.UNCONDITIONAL_PREDICATE;
import static io.pravega.controller.util.RetryHelper.withRetries;
import static io.pravega.controller.util.RetryHelper.withRetriesAsync;

/**
 * This method implements a failover sweeper for requests posted via {@link StreamMetadataTasks} into request stream.
 * Stream metadata task indexes the request in hostRequestIndex before initiating the work in metadata store.
 * If controller fails before the event is posted, the sweeper will be invoked upon failover.
 * The sweeper fetches indexed requests and posts corresponding event into request stream.
 *
 * This class wait before becoming ready until {@link StreamMetadataTasks} has its event writer set up.
 */
@Slf4j
public class RequestSweeper implements FailoverSweeper {

    public static final int LIMIT = 100;

    private final StreamMetadataStore metadataStore;
    private final ScheduledExecutorService executor;
    private final StreamMetadataTasks streamMetadataTasks;
    private final int limit;

    /**
     * Constructor for RequestSweeper object.
     *
     * @param metadataStore stream metadata store
     * @param executor executor
     * @param streamMetadataTasks stream metadata tasks
     */
    public RequestSweeper(final StreamMetadataStore metadataStore,
                          final ScheduledExecutorService executor, final StreamMetadataTasks streamMetadataTasks) {
        this(metadataStore, executor, streamMetadataTasks, LIMIT);
    }

    /**
     * Constructor for RequestSweeper object.
     *
     * @param metadataStore stream metadata store
     * @param executor executor
     * @param streamMetadataTasks stream metadata tasks
     * @param limit limit on number of requests to sweep for a host in one iteration.
     */
    @VisibleForTesting
    RequestSweeper(final StreamMetadataStore metadataStore,
                          final ScheduledExecutorService executor, final StreamMetadataTasks streamMetadataTasks,
                          final int limit) {
        this.metadataStore = metadataStore;
        this.executor = executor;
        this.streamMetadataTasks = streamMetadataTasks;
        this.limit = limit;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public CompletableFuture<Void> sweepFailedProcesses(final Supplier<Set<String>> runningProcesses) {
        return withRetriesAsync(metadataStore::listHostsWithPendingTask, RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor)
                .thenComposeAsync(registeredHosts -> {
                    log.info("Hosts {} have ongoing tasks", registeredHosts);
                    registeredHosts.removeAll(withRetries(runningProcesses, UNCONDITIONAL_PREDICATE, Integer.MAX_VALUE));
                    log.info("Failed hosts {} have orphaned tasks", registeredHosts);
                    return Futures.allOf(registeredHosts.stream()
                                                        .map(this::handleFailedProcess).collect(Collectors.toList()));
                }, executor);
    }

    /**
     * This method is called whenever a node in the controller cluster dies. A ServerSet abstraction may be used as
     * a trigger to invoke this method with one of the dead hostId.
     * <p>
     * It sweeps through all unfinished tasks of failed host and attempts to execute them to completion.
     * @param oldHostId old host id
     * @return A future that completes when sweeping completes
     */
    @Override
    public CompletableFuture<Void> handleFailedProcess(final String oldHostId) {
        log.info("Sweeping orphaned tasks for host {}", oldHostId);
        return withRetriesAsync(() -> Futures.doWhileLoop(
                () -> postRequest(oldHostId),
                list -> !list.isEmpty(), executor).whenCompleteAsync((result, ex) ->
                        log.info("Sweeping orphaned tasks for host {} complete", oldHostId), executor),
                RETRYABLE_PREDICATE, Integer.MAX_VALUE, executor);
    }

    @VisibleForTesting
    CompletableFuture<List<String>> postRequest(final String oldHostId) {
        return metadataStore.getPendingsTaskForHost(oldHostId, limit)
                            .thenComposeAsync(tasks -> Futures.allOfWithResults(
                                    tasks.entrySet().stream().map(entry ->
                                            streamMetadataTasks.writeEvent(entry.getValue())
                                                               .thenCompose(v ->
                                                                       streamMetadataTasks.removeTaskFromIndex(oldHostId, entry.getKey()))
                                                               .thenApply(v -> entry.getKey()))
                                         .collect(Collectors.toList())), executor);
    }
}
