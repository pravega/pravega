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

import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.TestStreamStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.shared.protocol.netty.WireCommandType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryOneTime;

import org.junit.Assert;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;
import org.junit.ClassRule;

import java.util.UUID;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Zookeeper based stream metadata store tests.
 */
public class PravegaTablesKVTMetadataStoreTest extends KVTableMetadataStoreTest {

    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(8000, 5000, RETRY_POLICY);
    private SegmentHelper segmentHelperMockForTables;

    @Override
    public void setupStore() throws Exception {
        segmentHelperMockForTables = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        // setup Stream Store, needed for creating scopes
        streamStore = StreamStoreFactory.createPravegaTablesStore(segmentHelperMockForTables, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        // setup KVTable Store
        store = KVTableStoreFactory.createPravegaTablesStore(segmentHelperMockForTables, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
        streamStore.close();
    }

    @Override
    Controller.CreateScopeStatus createScope(String scopeName) throws Exception {
        return streamStore.createScope(scopeName, null, executor).get();
    }

    @Test(timeout = 30000)
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        streamStore.createScope(scope, null, executor).get();
        UUID id = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable1, id, null, executor).get();
        store.createKeyValueTable(scope, kvtable1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, kvtable1, KVTableState.CREATING, null, executor).get();

        AssertExtensions.assertFutureThrows("Should throw IllegalStateException",
                store.getActiveSegments(scope, kvtable1, null, executor),
                (Throwable t) -> t instanceof StoreException.IllegalStateException);
    }

    @Test(timeout = 30000)
    public void testPartiallyDeletedScope() throws Exception {
        final String scopeName = "partialScope";

        PravegaTablesStoreHelper storeHelperSpy = spy(new PravegaTablesStoreHelper(segmentHelperMockForTables, GrpcAuthHelper.getDisabledAuthHelper(), executor));
        WireCommandFailedException wcfe = new WireCommandFailedException(WireCommandType.READ_TABLE_KEYS, WireCommandFailedException.Reason.TableKeyDoesNotExist);
        when(storeHelperSpy.getKeysPaginated(anyString(), any(), anyInt(), anyLong()))
            .thenReturn(CompletableFuture.failedFuture(new CompletionException(StoreException.create(StoreException.Type.DATA_NOT_FOUND, wcfe, "kvTablesInScope not found."))));
        StreamMetadataStore testStreamStore = TestStreamStoreFactory.createPravegaTablesStreamStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor, storeHelperSpy);
        KVTableMetadataStore testKVStore = TestStreamStoreFactory.createPravegaTablesKVStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor, storeHelperSpy);

        OperationContext context = testStreamStore.createScopeContext(scopeName, 0L);
        CompletableFuture<Controller.CreateScopeStatus> createScopeFuture = testStreamStore.createScope(scopeName, context, executor);
        Controller.CreateScopeStatus status = createScopeFuture.get();
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, status.getStatus());

        String token = Controller.ContinuationToken.newBuilder().build().getToken();
        Pair<List<String>, String> kvtList = testKVStore.listKeyValueTables(scopeName, token, 2, context, executor).get();
        Assert.assertEquals(0, kvtList.getKey().size());
        Assert.assertEquals(token, kvtList.getValue());
    }
}