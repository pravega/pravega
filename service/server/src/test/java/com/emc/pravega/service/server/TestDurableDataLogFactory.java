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

package com.emc.pravega.service.server;

import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogFactory;

import java.util.function.Consumer;

/**
 * DurableDataLogFactory for TestDurableDataLog objects.
 */
public class TestDurableDataLogFactory implements DurableDataLogFactory {
    private final DurableDataLogFactory wrappedFactory;
    private final Consumer<TestDurableDataLog> durableLogCreated;

    public TestDurableDataLogFactory(DurableDataLogFactory wrappedFactory) {
        this(wrappedFactory, null);
    }

    public TestDurableDataLogFactory(DurableDataLogFactory wrappedFactory, Consumer<TestDurableDataLog>
            durableLogCreated) {
        this.wrappedFactory = wrappedFactory;
        this.durableLogCreated = durableLogCreated;
    }

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        DurableDataLog log = this.wrappedFactory.createDurableDataLog(containerId);
        TestDurableDataLog result = TestDurableDataLog.create(log);
        if (this.durableLogCreated != null) {
            this.durableLogCreated.accept(result);
        }

        return result;
    }

    @Override
    public void close() {
        this.wrappedFactory.close();
    }
}
