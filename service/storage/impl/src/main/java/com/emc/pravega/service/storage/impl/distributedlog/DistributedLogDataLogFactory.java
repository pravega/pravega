/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.DurableDataLogFactory;

/**
 * Represents a DurableDataLogFactory that creates and manages instances of DistributedLogDataLog instances.
 */
public class DistributedLogDataLogFactory implements DurableDataLogFactory {
    private final LogClient client;

    /**
     * Creates a new instance of the DistributedLogDataLogFactory class.
     *
     * @param clientId The Id of the client to set for the DistributedLog client.
     * @param config   DistributedLog configuration.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the clientId is invalid.
     */
    public DistributedLogDataLogFactory(String clientId, DistributedLogConfig config) {
        this.client = new LogClient(clientId, config);
    }

    /**
     * Initializes this instance of the DistributedLogDataLogFactory.
     *
     * @throws IllegalStateException   If the DistributedLogDataLogFactory is already initialized.
     * @throws DurableDataLogException If an exception is thrown during initialization. The actual exception thrown may
     *                                 be a derived exception from this one, which provides more information about
     *                                 the failure reason.
     */
    public void initialize() throws DurableDataLogException {
        this.client.initialize();
    }

    //region DurableDataLogFactory Implementation

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        String logName = ContainerToLogNameConverter.getLogName(containerId);
        return new DistributedLogDataLog(logName, this.client);
    }

    @Override
    public void close() {
        this.client.close();
    }

    //endregion
}
