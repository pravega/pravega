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

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Stream metadata test.
 */
public abstract class KVTableMetadataStoreTest {
    protected KVTableMetadataStore store;
    protected StreamMetadataStore streamStore;
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected final String scope = "storescope";
    protected final String kvtable1 = "kvt1";
    protected final String kvtable2 = "kvt2";
    protected final KeyValueTableConfiguration configuration1 = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
    protected final KeyValueTableConfiguration configuration2 = KeyValueTableConfiguration.builder().partitionCount(3).primaryKeyLength(4).secondaryKeyLength(4).build();

    @Before
    public abstract void setupStore() throws Exception;

    @After
    public abstract void cleanupStore() throws Exception;

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    abstract Controller.CreateScopeStatus createScope(String scopeName) throws Exception;

    @Test(timeout = 30000)
    public void testKVTableMetadataStore() throws Exception {
        Controller.CreateScopeStatus scopeCreateStatus = createScope(scope);
        assertTrue(scopeCreateStatus.getStatus().equals(Controller.CreateScopeStatus.Status.SUCCESS)
                || scopeCreateStatus.getStatus().equals(Controller.CreateScopeStatus.Status.SCOPE_EXISTS));

        UUID id = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable1, id, null, executor).get();
        long start = System.currentTimeMillis();
        store.createKeyValueTable(scope, kvtable1, configuration1, start, null, executor).get();
        store.setState(scope, kvtable1, KVTableState.ACTIVE, null, executor).get();

        id = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable2, id, null, executor).get();
        store.createKeyValueTable(scope, kvtable2, configuration2, start, null, executor).get();
        store.setState(scope, kvtable2, KVTableState.ACTIVE, null, executor).get();

        assertEquals(configuration1, store.getConfiguration(scope, kvtable1, null, executor).get());
        // endregion

        // region checkSegments
        List<KVTSegmentRecord> segments = store.getActiveSegments(scope, kvtable1, null, executor).get();
        assertEquals(2, segments.size());

        segments = store.getActiveSegments(scope, kvtable2, null, executor).get();
        assertEquals(3, segments.size());
        // endregion
    }

    @Test(timeout = 30000)
    public void listTablesInScope() throws Exception {
        // list KeyValueTables in scope
        Controller.CreateScopeStatus scopeCreateStatus = createScope(scope);
        assertTrue(scopeCreateStatus.getStatus().equals(Controller.CreateScopeStatus.Status.SUCCESS)
                || scopeCreateStatus.getStatus().equals(Controller.CreateScopeStatus.Status.SCOPE_EXISTS));

        UUID id = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable1, id, null, executor).get();
        long start = System.currentTimeMillis();
        store.createKeyValueTable(scope, kvtable1, configuration1, start, null, executor).get();
        store.setState(scope, kvtable1, KVTableState.ACTIVE, null, executor).get();

        id = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable2, id, null, executor).get();
        store.createKeyValueTable(scope, kvtable2, configuration2, start, null, executor).get();
        store.setState(scope, kvtable2, KVTableState.ACTIVE, null, executor).get();

        Pair<List<String>, String> kvTablesInScope = store.listKeyValueTables(scope,
                                                    Controller.ContinuationToken.newBuilder().build().getToken(),
                                                    2, null, executor).get();

        assertEquals("List kvtables in scope", 2, kvTablesInScope.getKey().size());
        assertTrue("Found KVTable1", kvTablesInScope.getKey().contains(kvtable1));
        assertTrue("Found KVTable2", kvTablesInScope.getKey().contains(kvtable2));

        // List streams in non-existent scope 'Scope1'
        try {
            store.listKeyValueTables("Scope1", Controller.ContinuationToken.newBuilder().build().getToken(),
                    2, null, executor).join();
        } catch (StoreException se) {
            assertTrue("List streams in non-existent scope Scope1",
                    se instanceof StoreException.DataNotFoundException);
        } catch (CompletionException ce) {
            assertTrue("List streams in non-existent scope Scope1",
                    Exceptions.unwrap(ce) instanceof StoreException.DataNotFoundException);
        }
    }

    @Test(timeout = 30000)
    public void deleteKeyValueTableTest() throws Exception {
        final String scopeName = "ScopeDelete";
        final String kvtName = "KVTableDelete";
        KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(3).primaryKeyLength(4).secondaryKeyLength(4).build();

        // create KeyValueTable in scope
        Controller.CreateScopeStatus scopeCreateStatus = createScope(scopeName);
        assertEquals(scopeCreateStatus.getStatus(), Controller.CreateScopeStatus.Status.SUCCESS);

        UUID id = store.newScope(scope).newId();
        store.createEntryForKVTable(scopeName, kvtName, id, null, executor).get();
        long start = System.currentTimeMillis();
        store.createKeyValueTable(scopeName, kvtName, config, start, null, executor).get();
        store.setState(scopeName, kvtName, KVTableState.ACTIVE, null, executor).get();
        assertTrue(store.checkTableExists(scopeName, kvtName, null, executor).join());
        store.deleteKeyValueTable(scopeName, kvtName, null, executor).get();
        assertFalse(store.checkTableExists(scopeName, kvtName, null, executor).join());
    }
}
