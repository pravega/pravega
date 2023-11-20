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


import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.lang.Int96;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import java.util.concurrent.CompletableFuture;
import static io.pravega.shared.NameUtils.TRANSACTION_ID_COUNTER_TABLE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * PravegaTables based counter tests.
 */

public class PravegaTablesInt96CounterTest extends Int96CounterTest {
    private PravegaTablesStoreHelper storeHelper;

    @Override
    public void setupStore() throws Exception {
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        GrpcAuthHelper authHelper = GrpcAuthHelper.getDisabledAuthHelper();
        storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, authHelper, executor));
    }

    @Override
    public void cleanupStore() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Override
    Int96Counter getInt96Counter() {
        return spy(new PravegaTablesInt96Counter(storeHelper));
    }

    @Override
    Int96 getCounterForTesting(Int96Counter int96Counter) {
        return ((PravegaTablesInt96Counter) int96Counter).getCounterForTesting();
    }

    @Override
    Int96 getLimitForTesting(Int96Counter int96Counter) {
        return ((PravegaTablesInt96Counter) int96Counter).getLimitForTesting();
    }

    @Override
    CompletableFuture<Void> getRefreshFuture(Int96Counter int96Counter, OperationContext context) {
        return ((PravegaTablesInt96Counter) int96Counter).getRefreshFuture(context);
    }

    @Override
    void setCounterAndLimitForTesting(int counterMsb, long counterLsb, int limitMsb, long limitLsb, Int96Counter counter) {
        ((PravegaTablesInt96Counter) counter).setCounterAndLimitForTesting(counterMsb, counterLsb, limitMsb, limitLsb);
    }

    @Override
    void mockCounterValue() {
        // set range in store to have lsb = Long.Max - 100
        VersionedMetadata<Int96> data = new VersionedMetadata<>(new Int96(0, Long.MAX_VALUE - 100),
                new Version.LongVersion(2L));
        doReturn(CompletableFuture.completedFuture(data)).when(storeHelper).getEntry(eq(TRANSACTION_ID_COUNTER_TABLE),
                eq(PravegaTablesInt96Counter.COUNTER_KEY), any(), anyLong());
    }

    @Override
    void verifyRefreshRangeIfNeededCall(Int96Counter int96Counter, OperationContext context) {
        verify((PravegaTablesInt96Counter) int96Counter, times(3)).refreshRangeIfNeeded(context);
    }

    @Override
    void verifyGetRefreshFutureCall(Int96Counter int96Counter, OperationContext context) {
        verify((PravegaTablesInt96Counter) int96Counter, times(2)).getRefreshFuture(context);
    }
}