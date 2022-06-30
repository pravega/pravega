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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.test.common.AssertExtensions;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PravegaTablesStoreHelperTest {
    private ScheduledExecutorService executor;
    private SegmentHelper segmentHelper;
    private GrpcAuthHelper authHelper;
    private PravegaTablesStoreHelper storeHelper;

    @Before
    public void setup() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testTables() {
        // create table
        String table = "table";
        storeHelper.createTable(table, 0L).join();
        String key = "key";
        String value = "value";
        byte[] valueBytes = value.getBytes();
        storeHelper.addNewEntry(table, key, value, String::getBytes, 0L).join();

        // get entry
        VersionedMetadata<String> entry = storeHelper.getEntry(table, key, String::new, 0L).join();
        assertEquals(entry.getObject(), value);
        
        List<String> keys = new ArrayList<>();
        // get all keys
        storeHelper.getAllKeys(table, 0L).collectRemaining(keys::add).join();
        assertEquals(keys.size(), 1);
        assertEquals(keys.get(0), key);
        
        // get all entries
        Map<String, String> entries = new HashMap<>();
        storeHelper.getAllEntries(table, String::new, 0L).collectRemaining(x -> {
            entries.put(x.getKey(), x.getValue().getObject());
            return true;
        }).join();
        assertEquals(entries.size(), 1);        
        assertEquals(entries.get(key), value);        
        
        // update entry
        value = "value2";
        valueBytes = value.getBytes();
        Version version = entry.getVersion();
        storeHelper.updateEntry(table, key, value, String::getBytes, version, 0L).join();
        // bad version update
        AssertExtensions.assertFutureThrows("bad version", storeHelper.updateEntry(table, key, value,
                String::getBytes, version, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        // get and verify
        entry = storeHelper.getEntry(table, key, String::new, 0L).join();
        assertEquals(entry.getObject(), value);

        // check delete non empty table
        AssertExtensions.assertFutureThrows("Not Empty", storeHelper.deleteTable(table, true, 0L),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException);

        // conditional remove entry.
        AssertExtensions.assertFutureThrows("", storeHelper.removeEntry(table, key, new Version.LongVersion(123L), 0L),
                                            e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        // remove entry
        storeHelper.removeEntry(table, key, 0L).join();
        AssertExtensions.assertFutureThrows("", storeHelper.getEntry(table, key, String::new, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // idempotent remove
        storeHelper.removeEntry(table, key, 0L).join();
        
        storeHelper.addNewEntryIfAbsent(table, key, value, String::getBytes, 0L).join();
        entry = storeHelper.getEntry(table, key, String::new, 0L).join();
        assertEquals(entry.getObject(), value);
        version = entry.getVersion();
        
        // idempotent
        storeHelper.addNewEntryIfAbsent(table, key, value, x -> x.getBytes(), 0L).join();
        entry = storeHelper.getEntry(table, key, String::new, 0L).join();
        assertEquals(entry.getVersion(), version);
        
        AssertExtensions.assertFutureThrows("Exists", storeHelper.addNewEntry(table, key, value, x -> x.getBytes(), 0L),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException);

        List<Map.Entry<String, byte[]>> entriesToAdd = new ArrayList<>();
        entriesToAdd.add(new AbstractMap.SimpleEntry<>("1", new byte[0]));
        entriesToAdd.add(new AbstractMap.SimpleEntry<>("2", new byte[0]));
        entriesToAdd.add(new AbstractMap.SimpleEntry<>("3", new byte[0]));
        storeHelper.addNewEntriesIfAbsent(table, entriesToAdd, x -> x, 0L).join();

        keys = new ArrayList<>();
        // get all keys
        storeHelper.getAllKeys(table, 0L).collectRemaining(keys::add).join();
        assertEquals(keys.size(), 4);
        
        // get all keys paginated
        ByteBuf token = Unpooled.wrappedBuffer(Base64.getDecoder().decode(""));
        Map.Entry<ByteBuf, List<String>> response = storeHelper.getKeysPaginated(table, token, 2, 0L).join();
        assertEquals(response.getValue().size(), 2);
        assertTrue(response.getKey().hasArray());

        response = storeHelper.getKeysPaginated(table, response.getKey(), 2, 0L).join();
        assertEquals(response.getValue().size(), 2);
        assertTrue(response.getKey().hasArray());

        // remove entries
        storeHelper.removeEntries(table, Lists.newArrayList("1", "2", "3", key), 0L).join();

        // non existent table
        AssertExtensions.assertFutureThrows("non existent table", storeHelper.getEntry("nonExistentTable", key, x -> x, 0L),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // non existent key
        AssertExtensions.assertFutureThrows("non existent key", storeHelper.getEntry(table, "nonExistentKey", x -> x, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        keys = Lists.newArrayList("4", "5", "non existent", "7");
        entriesToAdd = new ArrayList<>();
        entriesToAdd.add(new AbstractMap.SimpleEntry<>(keys.get(0), keys.get(0).getBytes()));
        entriesToAdd.add(new AbstractMap.SimpleEntry<>(keys.get(1), keys.get(1).getBytes()));
        entriesToAdd.add(new AbstractMap.SimpleEntry<>(keys.get(3), keys.get(3).getBytes()));
        storeHelper.addNewEntriesIfAbsent(table, entriesToAdd, x -> x, 0L).join();

        Version.LongVersion nonExistentKey = new Version.LongVersion(-1);
        List<VersionedMetadata<String>> values = storeHelper.getEntries(table, keys,
                String::new, new VersionedMetadata<>(null, nonExistentKey), 0L).join();
        assertEquals(keys.size(), values.size());
        assertEquals(keys.get(0), values.get(0).getObject());
        assertEquals(keys.get(1), values.get(1).getObject());
        assertSame(values.get(2).getVersion().asLongVersion(), nonExistentKey);
        assertEquals(keys.get(3), values.get(3).getObject());
    }

    @Test
    public void testRetriesExhausted() {
        SegmentHelper segmentHelper = spy(SegmentHelperMock.getSegmentHelperMockForTables(executor));
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor, 2);

        CompletableFuture<Void> connectionDropped = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.ConnectionDropped));
        doAnswer(x -> connectionDropped).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());
        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.createTable("table", 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        CompletableFuture<Void> connectionFailed = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.ConnectionFailed));
        doAnswer(x -> connectionFailed).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());
        AssertExtensions.assertFutureThrows("ConnectionFailed", storeHelper.createTable("table", 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        CompletableFuture<Void> authFailed = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.AuthFailed));
        doAnswer(x -> connectionFailed).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());
        AssertExtensions.assertFutureThrows("AuthFailed", storeHelper.createTable("table", 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
    }
    
    @Test
    public void testNoRetriesOnUpdate() {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        PravegaTablesStoreHelper storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, authHelper, executor, 2));

        // region connection dropped
        CompletableFuture<Void> connectionDropped = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.UPDATE_TABLE_ENTRIES, WireCommandFailedException.Reason.ConnectionDropped));
        doAnswer(x -> connectionDropped).when(segmentHelper).updateTableEntries(anyString(), any(), anyString(), anyLong());
        
        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.addNewEntry("table", "key",
                new byte[0], x -> x, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        verify(segmentHelper, times(1)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.updateEntry("table", "key",
                new byte[0], x -> x, new Version.LongVersion(0L), 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        verify(segmentHelper, times(2)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        // endregion
        
        // region connectionfailed
        CompletableFuture<Void> connectionFailed = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.UPDATE_TABLE_ENTRIES, WireCommandFailedException.Reason.ConnectionFailed));
        doAnswer(x -> connectionFailed).when(segmentHelper).updateTableEntries(anyString(), any(), anyString(), anyLong());

        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.addNewEntry("table", "key",
                new byte[0], x -> x, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        verify(segmentHelper, times(3)).updateTableEntries(anyString(), any(), anyString(), anyLong());
        
        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.updateEntry("table", "key",
                new byte[0], x -> x, new Version.LongVersion(0L), 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        verify(segmentHelper, times(4)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        // endregion
        
        CompletableFuture<Void> unknownHost = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.UPDATE_TABLE_ENTRIES, WireCommandFailedException.Reason.UnknownHost));
        doAnswer(x -> unknownHost).when(segmentHelper).updateTableEntries(anyString(), any(), anyString(), anyLong());
        
        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.addNewEntry("table", "key",
                new byte[0], x -> x, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        // this should be retried. we have configured 2 retries, so 2 retries should happen hence jump from 4 to 6. 
        verify(segmentHelper, times(6)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.updateEntry("table", "key",
                new byte[0], x -> x, new Version.LongVersion(0L), 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        verify(segmentHelper, times(8)).updateTableEntries(anyString(), any(), anyString(), anyLong());
    }

    /*
     * Test that verifies controller should not retry when Segment store send the reply back with NoSuchSegment WireCommand.
     */
    @Test
    public void testNoRetriesOnNoSuchSegmentException() {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        PravegaTablesStoreHelper storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, authHelper, executor, 2));

        // region noSuchSegment exception is not retried
        CompletableFuture<Void> noSuchSegment = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.UPDATE_TABLE_ENTRIES, WireCommandFailedException.Reason.SegmentDoesNotExist));
        doAnswer(x -> noSuchSegment).when(segmentHelper).updateTableEntries(anyString(), any(), anyString(), anyLong());

        //this should not be retried. we have configured 2 retries, but as there is no retry happens hence count will be 1.
        AssertExtensions.assertFutureThrows("noSuchSegment", storeHelper.addNewEntry("table", "key",
                        new byte[0], x -> x, 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataContainerNotFoundException);
        verify(segmentHelper, times(1)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        AssertExtensions.assertFutureThrows("noSuchSegment", storeHelper.updateEntry("table", "key",
                        new byte[0], x -> x, new Version.LongVersion(0L), 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataContainerNotFoundException);
        verify(segmentHelper, times(2)).updateTableEntries(anyString(), any(), anyString(), anyLong());

        // endregion
    }
}
