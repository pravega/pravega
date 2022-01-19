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
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.ZKScope;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * ZK kvtable metadata store.
 */
@Slf4j
public class ZookeeperKVTMetadataStore extends AbstractKVTableMetadataStore implements AutoCloseable {
    @VisibleForTesting
    static final String SCOPE_ROOT_PATH = "/store";
    static final String SCOPE_DELETE_PATH = "_system/deletingScopes";
    static final String DELETED_KVTABLES_PATH = "/lastActiveKVTableSegment/%s";

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ZKStoreHelper storeHelper;
    private final Executor executor;

    @VisibleForTesting
    public ZookeeperKVTMetadataStore(CuratorFramework client, Executor executor) {
        super(new ZKHostIndex(client, "/hostKVTRequestIndex", executor));
        this.storeHelper = new ZKStoreHelper(client, executor);
        this.executor = executor;
    }

    @Override
    ZookeeperKVTable newKeyValueTable(final String scope, final String name) {
        return new ZookeeperKVTable(scope, name, storeHelper, executor);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor) {
        String scopePath = ZKPaths.makePath(SCOPE_ROOT_PATH, scope);
        return storeHelper.checkExists(scopePath);
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext ctx, Executor executor) {
        String scopePath = ZKPaths.makePath(SCOPE_DELETE_PATH, scope);
        return storeHelper.checkExists(scopePath);
    }

    @Override
    public CompletableFuture<Boolean> checkTableExists(String scopeName, String kvt, OperationContext context, Executor executor) {
        return Futures.completeOn(((ZKScope) getScope(scopeName, context)).checkKeyValueTableExistsInScope(kvt), executor);
    }

    @Override
    public CompletableFuture<Void> createEntryForKVTable(String scopeName, String kvtName, UUID id, OperationContext context,
                                                         Executor executor) {
        return Futures.completeOn(
                CompletableFuture.completedFuture((ZKScope) getScope(scopeName, context))
                 .thenCompose(scope -> {
                     scope.addKVTableToScope(kvtName, id);
                     return CompletableFuture.completedFuture(null);
                 }), executor);
    }

    @Override
    public ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(String scope, String name, KeyValueTableConfiguration configuration,
                                                                long createTimestamp, OperationContext context, Executor executor) {
        return super.createKeyValueTable(scope, name, configuration, createTimestamp, context, executor);
    }

    @Override
    public CompletableFuture<Void> deleteFromScope(final String scope,
                                                   final String name,
                                                   final OperationContext context,
                                                   final Executor executor) {
        return Futures.completeOn(((ZKScope) getScope(scope, context)).removeKVTableFromScope(name),
                executor);
    }


    @Override
    CompletableFuture<Void> recordLastKVTableSegment(String scope, String kvtable, int lastActiveSegment, OperationContext context, Executor executor) {
        final String deletePath = String.format(DELETED_KVTABLES_PATH, getScopedKVTName(scope, kvtable));
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return storeHelper.getData(deletePath, x -> BitConverter.readInt(x, 0))
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                })
                .thenCompose(data -> {
                    log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, kvtable);
                    if (data == null) {
                        return Futures.toVoid(storeHelper.createZNodeIfNotExist(deletePath, maxSegmentNumberBytes));
                    } else {
                        final int oldLastActiveSegment = data.getObject();
                        Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                oldLastActiveSegment, scope, kvtable, lastActiveSegment);
                        return Futures.toVoid(storeHelper.setData(deletePath, maxSegmentNumberBytes, data.getVersion()));
                    }
                });
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName, 
                                                                      OperationContext operationContext, Executor executor) {
        return storeHelper.getData(String.format(DELETED_KVTABLES_PATH, getScopedKVTName(scopeName, streamName)), x -> BitConverter.readInt(x, 0))
                          .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedKVTName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }

    @Override
    public void close() {
    }
    // endregion
}
