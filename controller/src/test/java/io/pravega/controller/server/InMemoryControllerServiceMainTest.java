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
package io.pravega.controller.server;

import io.pravega.controller.store.client.impl.StoreClientConfigImpl;

/**
 * In-memory store based ControllerServiceMain tests.
 */
public class InMemoryControllerServiceMainTest extends ControllerServiceMainTest {
    public InMemoryControllerServiceMainTest() {
        super(true);
    }

    @Override
    public void setup() {
        storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
    }

    @Override
    public void tearDown() {
    }
}
