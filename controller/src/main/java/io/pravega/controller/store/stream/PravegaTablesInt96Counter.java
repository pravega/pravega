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

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.ZKStoreHelper;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;

import static io.pravega.shared.NameUtils.TRANSACTION_ID_COUNTER_TABLE;

/**
 * A Pravega tables based int 96 counter. At bootstrap, and upon each refresh, it retrieves a unique starting counter from the
 * Int96 space. It does this by getting and updating an Int96 value stored in Pravega Table. It updates the table value
 * with the next highest value that others could access.
 * So upon each retrieval from Pravega table, it has a starting counter value and a ceiling on counters it can assign until a refresh
 * is triggered. Any caller requesting for nextCounter will get a unique value from the range owned by this counter.
 * Once it exhausts the range, it refreshes the range by contacting Pravega Tables and repeating the steps described above.
 */

class PravegaTablesInt96Counter extends AbstractInt96Counter {
    static final String COUNTER_KEY = "counter";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesInt96Counter.class));
    private final PravegaTablesStoreHelper storeHelper;
    private final ZKStoreHelper zkStoreHelper;

    public PravegaTablesInt96Counter(final PravegaTablesStoreHelper storeHelper, final ZKStoreHelper zkStoreHelper) {
        this.storeHelper = storeHelper;
        this.zkStoreHelper = zkStoreHelper;
    }

    @Override
    CompletableFuture<Void> getRefreshFuture(final OperationContext context) {
        return getCounterFromTable(context).thenCompose(data -> {
            Int96 previous = data.getObject();
            Int96 nextLimit = previous.add(COUNTER_RANGE);
            return storeHelper.updateEntry(TRANSACTION_ID_COUNTER_TABLE, COUNTER_KEY, nextLimit,
                            Int96::toBytes, data.getVersion(), context.getRequestId())
                    .thenAccept(x -> reset(previous, nextLimit, context));
        });
    }

    private CompletableFuture<VersionedMetadata<Int96>> getCounterFromTable(final OperationContext context) {
        return Futures.exceptionallyComposeExpecting(storeHelper.getEntry(TRANSACTION_ID_COUNTER_TABLE,
                        COUNTER_KEY, Int96::fromBytes, context.getRequestId()),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                () -> storeHelper.createTable(TRANSACTION_ID_COUNTER_TABLE, context.getRequestId())
                        .thenCompose(v -> {
                            log.info(context.getRequestId(), "batches root table {} created", TRANSACTION_ID_COUNTER_TABLE);
                            return getCounterFromZK();
                        }).thenCompose(v -> storeHelper.addNewEntryIfAbsent(TRANSACTION_ID_COUNTER_TABLE,
                                COUNTER_KEY, v.getObject().compareTo(Int96.ZERO) == 0 ? Int96.ZERO : v.getObject(),
                                Int96::toBytes, context.getRequestId()))
                        .thenCompose(v -> storeHelper.getEntry(TRANSACTION_ID_COUNTER_TABLE,
                                COUNTER_KEY, Int96::fromBytes, context.getRequestId())));
    }

    private CompletableFuture<VersionedMetadata<Int96>> getCounterFromZK() {
        return zkStoreHelper.getData(COUNTER_PATH, Int96::fromBytes)
                .exceptionally(e -> {
                   log.warn("Exception while reading the counter value from zookeeper store.", e);
                   return new VersionedMetadata<>(Int96.ZERO, new Version.IntVersion(0));
                });
    }


}