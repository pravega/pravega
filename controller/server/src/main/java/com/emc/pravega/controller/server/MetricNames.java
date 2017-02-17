/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

public interface MetricNames {

    // Stream request counts (Static)
    public final static String CREATE_STREAM = "CREATE_STREAM";
    public final static String SEAL_STREAM = "SEAL_STREAM";
    public final static String DELETE_STREAM = "DELETE_STREAM";
    
    // Transaction request Operations (Dynamic)
    public final static String CREATE_TRANSACTION = "CREATE_TRANSACTION";
    public final static String COMMIT_TRANSACTION = "COMMIT_TRANSACTION";
    public final static String ABORT_TRANSACTION = "ABORT_TRANSACTION";
    public final static String TIMEOUT_TRANSACTION = "TIMEOUT_TRANSACTION";
    public final static String OPEN_TRANSACTIONS = "OPEN_TRANSACTIONS"; // Gauge
    
    static String nameFromStream(String metric, String scope, String stream) {
        return metric + "." + scope + "." + stream;
    }
}
