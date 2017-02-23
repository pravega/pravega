/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

public interface MetricNames {

    // Stream request counts (Static)
    String CREATE_STREAM = "CREATE_STREAM";
    String SEAL_STREAM = "SEAL_STREAM";
    String DELETE_STREAM = "DELETE_STREAM";
    
    // Transaction request Operations (Dynamic)
    String CREATE_TRANSACTION = "CREATE_TRANSACTION";
    String COMMIT_TRANSACTION = "COMMIT_TRANSACTION";
    String ABORT_TRANSACTION = "ABORT_TRANSACTION";
    String TIMEOUT_TRANSACTION = "TIMEOUT_TRANSACTION";
    String OPEN_TRANSACTIONS = "OPEN_TRANSACTIONS"; // Gauge

    // Stream segment counts (Dynamic)
    String NUMBER_OF_SEGMENTS = "NUMBER_OF_SEGMENTS"; //Counter
    String SPLITS = "SPLITS"; // Guage
    String MERGES = "MERGES"; // Guage

    static String nameFromStream(String metric, String scope, String stream) {
        return metric + "." + scope + "." + stream;
    }
}
