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
package io.pravega.controller.store;

import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PravegaTablesScopeTest extends ThreadPooledTestSuite {
    private final String scope = "scope";
    private final String stream = "stream";
    private final String indexTable = "table";
    private final String tag = "tag";
    private final byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
    private final OperationContext context = new TestOperationContext();
    private List<TableSegmentKey> keySnapshot;

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void testRemoveTagsUnderScope() {
        // Setup Mocks.
        GrpcAuthHelper authHelper = mock(GrpcAuthHelper.class);
        when(authHelper.retrieveMasterToken()).thenReturn("");
        SegmentHelper segmentHelper = mock(SegmentHelper.class);
        PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executorService());
        PravegaTablesScope tablesScope = spy(new PravegaTablesScope(scope, storeHelper));
        doReturn(CompletableFuture.completedFuture(indexTable)).when(tablesScope)
                                                               .getAllStreamTagsInScopeTableNames(stream, context);
        // Simulate an empty value being returned.
        TableSegmentEntry entry = TableSegmentEntry.versioned(tagBytes, new byte[0], 1L);
        when(segmentHelper.readTable(eq(indexTable), any(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(singletonList(entry)));
        when(segmentHelper.updateTableEntries(eq(indexTable), any(), anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(singletonList(TableSegmentKeyVersion.from(2L))));
        when(segmentHelper.removeTableKeys(eq(indexTable), any(), anyString(), anyLong()))
                .thenAnswer(invocation -> {
                    //Capture the key value sent during removeTableKeys.
                    keySnapshot = (List<TableSegmentKey>) invocation.getArguments()[1];
                    return CompletableFuture.completedFuture(null);
                });

        // Invoke the removeTags method.
        tablesScope.removeTagsUnderScope(stream, Set.of(tag), context).join();
        // Verify if correctly detect that the data is empty and the entry is cleaned up.
        verify(segmentHelper, times(1)).removeTableKeys(eq(indexTable), eq(keySnapshot), anyString(), anyLong());
        // Verify if the version number is as expected.
        assertEquals(2L, keySnapshot.get(0).getVersion().getSegmentVersion());
    }

    @Test
    public void testDeleteScopeRecursive() {
        GrpcAuthHelper authHelper = mock(GrpcAuthHelper.class);
        when(authHelper.retrieveMasterToken()).thenReturn("");
        SegmentHelper segmentHelper = mock(SegmentHelper.class);
        PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executorService());
        PravegaTablesScope tablesScope = spy(new PravegaTablesScope(scope, storeHelper));
        tablesScope.deleteScopeRecursive(context);
        verify(tablesScope, times(1)).getStreamsInScopeTableName(true, context);
        verify(tablesScope, times(1)).getReaderGroupsInScopeTableName(context);
        verify(tablesScope, times(1)).getKVTablesInScopeTableName(context);
        verify(tablesScope, times(1)).getAllStreamTagsInScopeTableNames(context);
        tablesScope.sealScope(scope, context);
        verify(tablesScope, times(5)).getId(context);
    }
}