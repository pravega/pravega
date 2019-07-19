/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class PravegaTablesStoreHelperTest {
    private ScheduledExecutorService executor;
    private SegmentHelper segmentHelper;
    private AuthHelper authHelper;
    private PravegaTablesStoreHelper storeHelper;

    @Before
    public void setup() throws Exception {
        executor = Executors.newScheduledThreadPool(5);
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        authHelper = AuthHelper.getDisabledAuthHelper();
        storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        executor.shutdown();
    }

    @Test
    public void testTables() {
        // create table
        String table = "table";
        storeHelper.createTable(table).join();
        String key = "key";
        String value = "value";
        byte[] valueBytes = value.getBytes();
        storeHelper.addNewEntry(table, key, valueBytes).join();

        // get entry
        VersionedMetadata<String> entry = storeHelper.getEntry(table, key, String::new).join();
        assertEquals(entry.getObject(), value);
        
        List<String> keys = new ArrayList<>();
        // get all keys
        storeHelper.getAllKeys(table).collectRemaining(keys::add).join();
        assertEquals(keys.size(), 1);
        assertEquals(keys.get(0), key);
        
        // get all entries
        Map<String, String> entries = new HashMap<>();
        storeHelper.getAllEntries(table, String::new).collectRemaining(x -> {
            entries.put(x.getKey(), x.getValue().getObject());
            return true;
        }).join();
        assertEquals(entries.size(), 1);        
        assertEquals(entries.get(key), value);        
        
        // update entry
        value = "value2";
        valueBytes = value.getBytes();
        Version version = entry.getVersion();
        storeHelper.updateEntry(table, key, valueBytes, version).join();
        // bad version update
        AssertExtensions.assertFutureThrows("bad version", storeHelper.updateEntry(table, key, valueBytes, version),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        // get and verify
        entry = storeHelper.getEntry(table, key, String::new).join();
        assertEquals(entry.getObject(), value);

        // check delete non empty table
        AssertExtensions.assertFutureThrows("Not Empty", storeHelper.deleteTable(table, true), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotEmptyException);
        
        // remove entry
        storeHelper.removeEntry(table, key).join();
        AssertExtensions.assertFutureThrows("", storeHelper.getEntry(table, key, String::new), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // idempotent remove
        storeHelper.removeEntry(table, key).join();
        
        storeHelper.addNewEntryIfAbsent(table, key, valueBytes).join();
        entry = storeHelper.getEntry(table, key, String::new).join();
        assertEquals(entry.getObject(), value);
        version = entry.getVersion();
        
        // idempotent
        storeHelper.addNewEntryIfAbsent(table, key, valueBytes).join();
        entry = storeHelper.getEntry(table, key, String::new).join();
        assertEquals(entry.getVersion(), version);
        
        AssertExtensions.assertFutureThrows("Exists", storeHelper.addNewEntry(table, key, valueBytes), 
            e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException);

        Map<String, byte[]> entriesToAdd = new HashMap<>();
        entriesToAdd.put("1", new byte[0]);
        entriesToAdd.put("2", new byte[0]);
        entriesToAdd.put("3", new byte[0]);
        storeHelper.addNewEntriesIfAbsent(table, entriesToAdd).join();

        keys = new ArrayList<>();
        // get all keys
        storeHelper.getAllKeys(table).collectRemaining(keys::add).join();
        assertEquals(keys.size(), 4);
        
        // get all keys paginated
        ByteBuf token = Unpooled.wrappedBuffer(Base64.getDecoder().decode(""));
        Map.Entry<ByteBuf, List<String>> response = storeHelper.getKeysPaginated(table, token, 2).join();
        assertEquals(response.getValue().size(), 2);
        assertTrue(response.getKey().hasArray());

        response = storeHelper.getKeysPaginated(table, response.getKey(), 2).join();
        assertEquals(response.getValue().size(), 2);
        assertTrue(response.getKey().hasArray());

        // remove entries
        storeHelper.removeEntries(table, Lists.newArrayList("1", "2", "3", key)).join();

        // non existent table
        AssertExtensions.assertFutureThrows("non existent table", storeHelper.getEntry("nonExistentTable", key, x -> x),
            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // non existent key
        AssertExtensions.assertFutureThrows("non existent key", storeHelper.getEntry(table, "nonExistentKey", x -> x),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }

    @Test
    public void testRetriesExhausted() {
        SegmentHelper segmentHelper = spy(SegmentHelperMock.getSegmentHelperMockForTables(executor));
        AuthHelper authHelper = AuthHelper.getDisabledAuthHelper();
        PravegaTablesStoreHelper storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor, 2);

        CompletableFuture<Void> connectionDropped = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.ConnectionDropped));
        doAnswer(x -> connectionDropped).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong());
        AssertExtensions.assertFutureThrows("ConnectionDropped", storeHelper.createTable("table"),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        CompletableFuture<Void> connectionFailed = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.ConnectionFailed));
        doAnswer(x -> connectionFailed).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong());
        AssertExtensions.assertFutureThrows("ConnectionFailed", storeHelper.createTable("table"),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        CompletableFuture<Void> authFailed = Futures.failedFuture(
                new WireCommandFailedException(WireCommandType.CREATE_TABLE_SEGMENT, WireCommandFailedException.Reason.AuthFailed));
        doAnswer(x -> connectionFailed).when(segmentHelper).createTableSegment(anyString(), anyString(), anyLong());
        AssertExtensions.assertFutureThrows("AuthFailed", storeHelper.createTable("table"),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
    }
}
