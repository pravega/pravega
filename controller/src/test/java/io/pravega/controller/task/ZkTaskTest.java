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
package io.pravega.controller.task;


import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.test.common.SerializedClassRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;

/**
 * Task test cases.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class ZkTaskTest extends TaskTest {
    public ZkTaskTest() throws Exception {
    }

    @Override
    protected StreamMetadataStore getStream() {
        return StreamStoreFactory.createZKStore(cli, executor);
    }
}

