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

package com.emc.logservice.serverhost;

import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageimplementation.distributedlog.DistributedLogConfig;
import com.emc.logservice.storageimplementation.distributedlog.DistributedLogDataLogFactory;

import java.util.Properties;
import java.util.concurrent.CompletionException;

/**
 * Distributed-Log Service builder (that still writes to memory).
 */
public class DistributedLogServiceBuilder extends InMemoryServiceBuilder {

    public DistributedLogServiceBuilder(int containerCount) {
        super(containerCount);
    }

    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        DistributedLogConfig dlConfig;
        try {
            Properties dlProperties = new Properties();
            dlProperties.put("dlog.hostname", "localhost");
            dlProperties.put("dlog.port", "7000");
            dlProperties.put("dlog.namespace", "messaging/distributedlog");

            dlConfig = new DistributedLogConfig(dlProperties);
            DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory("interactive-console", dlConfig);
            factory.initialize();
            return factory;
        } catch (Exception ex) {
            throw new CompletionException(ex);
        }
    }
}
