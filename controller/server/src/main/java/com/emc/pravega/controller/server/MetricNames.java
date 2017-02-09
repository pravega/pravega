/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
