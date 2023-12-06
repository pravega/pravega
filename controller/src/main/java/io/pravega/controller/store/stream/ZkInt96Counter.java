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
package io.pravega.controller.store.stream;

import io.pravega.common.lang.Int96;
import io.pravega.controller.store.ZKStoreHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * A zookeeper based int 96 counter. At bootstrap, and upon each refresh, it retrieves a unique starting counter from the 
 * Int96 space. It does this by getting and updating an Int96 value stored in zookeeper. It updates zookeeper node with the 
 * next highest value that others could access. 
 * So upon each retrieval from zookeeper, it has a starting counter value and a ceiling on counters it can assign until a refresh
 * is triggered. Any caller requesting for nextCounter will get a unique value from the range owned by this counter. 
 * Once it exhausts the range, it refreshes the range by contacting zookeeper and repeating the steps described above. 
 */
@Slf4j
class ZkInt96Counter extends AbstractInt96Counter {
    private final ZKStoreHelper storeHelper;

    public ZkInt96Counter(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }


    @Override
    CompletableFuture<Void> getRefreshFuture(final OperationContext context) {
        return storeHelper
                .createZNodeIfNotExist(COUNTER_PATH, Int96.ZERO.toBytes())
                .thenCompose(v -> storeHelper.getData(COUNTER_PATH, Int96::fromBytes)
                           .thenCompose(data -> {
                               Int96 previous = data.getObject();
                               Int96 nextLimit = previous.add(COUNTER_RANGE);
                               return storeHelper.setData(COUNTER_PATH, nextLimit.toBytes(), data.getVersion())
                                     .thenAccept(x -> reset(previous, nextLimit, context));
                           }));
    }

}
