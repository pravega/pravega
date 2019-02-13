package io.pravega.controller.store.stream;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.stream.Data;

import java.util.concurrent.CompletableFuture;

public class PravegaTablesStoreHelper {
    private final String 
    
    private final SegmentHelper segmentHelper;

    public PravegaTablesStoreHelper(SegmentHelper segmentHelper) {
        this.segmentHelper = segmentHelper;
    }
    
    CompletableFuture<Boolean> createTable() {
        
    }

    CompletableFuture<Data> addData(String scope, String tableName, String key, byte[] value) {
    }
    
    CompletableFuture<Data> getData(String scope, String tableName, String key) {
        segmentHelper.readTable(scope, )
    }
    
    
}
