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
package io.pravega.controller.store.task;

import io.pravega.common.hash.RandomFactory;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.task.TaskData;
import com.google.common.base.Preconditions;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract task metadata store.
 */
public abstract class AbstractTaskMetadataStore implements TaskMetadataStore {

    private final static String TAG_SEPARATOR = "_%%%_";
    private final static String RESOURCE_PART_SEPARATOR = "_%_";
    protected final ScheduledExecutorService executor;
    private final HostIndex hostIndex;

    AbstractTaskMetadataStore(HostIndex hostIndex, ScheduledExecutorService executor) {
        this.hostIndex = hostIndex;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> lock(final Resource resource,
                                                     final TaskData taskData,
                                                     final String owner,
                                                     final String tag,
                                                     final String oldOwner,
                                                     final String oldTag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(taskData);
            Preconditions.checkNotNull(owner);
            Preconditions.checkArgument(!owner.isEmpty());
            Preconditions.checkNotNull(tag);
            Preconditions.checkArgument(!tag.isEmpty());
            Preconditions.checkArgument((oldOwner == null && oldTag == null) || (oldOwner != null && oldTag != null));
            Preconditions.checkArgument(oldOwner == null || !oldOwner.isEmpty());
            Preconditions.checkArgument(oldTag == null || !oldTag.isEmpty());

            if (oldOwner == null) {
                return acquireLock(resource, taskData, owner, tag);
            } else {
                return transferLock(resource, owner, tag, oldOwner, oldTag);
            }

        }, executor);

    }

    @Override
    public CompletableFuture<Void> unlock(final Resource resource,
                                                       final String owner,
                                                       final String tag) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(resource);
            Preconditions.checkNotNull(owner);
            Preconditions.checkNotNull(tag);

            return removeLock(resource, owner, tag);

        }, executor);
    }

    abstract Void acquireLock(final Resource resource,
                              final TaskData taskData,
                              final String owner,
                              final String threadId);

    abstract Void transferLock(final Resource resource,
                              final String owner,
                              final String threadId,
                              final String oldOwner,
                              final String oldThreadId);

    abstract Void removeLock(final Resource resource, final String owner, final String tag);

    @Override
    public CompletableFuture<Void> putChild(final String parent, final TaggedResource child) {
        return hostIndex.addEntity(parent, getNode(child));
    }

    @Override
    public CompletableFuture<Void> removeChild(final String parent,
                                               final TaggedResource child,
                                               final boolean deleteEmptyParent) {
        return hostIndex.removeEntity(parent, getNode(child), deleteEmptyParent);
    }

    @Override
    public CompletableFuture<Void> removeNode(final String parent) {
        return hostIndex.removeHost(parent);
    }

    @Override
    public CompletableFuture<Optional<TaggedResource>> getRandomChild(final String parent) {
        return hostIndex.getEntities(parent).thenApply(list -> list != null && list.size() > 0 ?
                Optional.of(this.getTaggedResource(list.get(RandomFactory.create().nextInt(list.size())))) : Optional.empty());
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return hostIndex.getHosts();
    }

    protected String getNode(final Resource resource) {
        return resource.getString().replaceAll("/", RESOURCE_PART_SEPARATOR);
    }

    protected String getNode(final TaggedResource resource) {
        return getNode(resource.getResource()) + TAG_SEPARATOR + resource.getTag();
    }

    protected Resource getResource(final String node) {
        String[] parts = node.split(RESOURCE_PART_SEPARATOR);
        return new Resource(parts);
    }

    protected TaggedResource getTaggedResource(final String node) {
        String[] splits = node.split(TAG_SEPARATOR);
        Preconditions.checkArgument(splits.length == 2, "Invalid TaggedResource node");
        return new TaggedResource(splits[1], getResource(splits[0]));
    }
}
