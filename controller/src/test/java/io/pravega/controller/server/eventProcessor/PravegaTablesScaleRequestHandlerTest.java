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
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.TestStreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.shared.controller.event.ScaleOpEvent;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;

public class PravegaTablesScaleRequestHandlerTest extends ScaleRequestHandlerTest {
    SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executor);
    PravegaTablesStoreHelper storeHelper;

    @Override
    <T> Number getVersionNumber(VersionedMetadata<T> versioned) {
        return (int) versioned.getVersion().asLongVersion().getLongValue();
    }

    @Override
    StreamMetadataStore getStore() {
        storeHelper = spy(new PravegaTablesStoreHelper(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), executor));
        return TestStreamStoreFactory.createPravegaTablesStreamStore(zkClient, executor, storeHelper);

    }

    @Test(timeout = 30000)
    public void testEpochMigration() throws ExecutionException, InterruptedException {
        final String scope = "scopeEpoch";
        streamStore.createScope(scope, null, executor).get();
        final String testStream = "streamEpoch";

        final String epoch0Key = "epochRecord-0";
        long creationTime = System.currentTimeMillis();
        StreamSegmentRecord segRecord = new StreamSegmentRecord(0, 0, creationTime, 0.0, 1.0);
        EpochRecord firstEpochInOldFormat = new EpochRecord(0, 0, ImmutableList.of(segRecord), creationTime,
                EpochRecord.DEFAULT_COUNT_VALUE, EpochRecord.DEFAULT_COUNT_VALUE);
        VersionedMetadata<EpochRecord> expectedEpochRecord = new VersionedMetadata<>(firstEpochInOldFormat, new Version.IntVersion(0));

        doReturn(CompletableFuture.completedFuture(expectedEpochRecord))
                .when(storeHelper).getCachedOrLoad(anyString(), eq(epoch0Key), any(), anyLong(), anyLong());

        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore.createStream(scope, testStream, config, System.currentTimeMillis(), null, executor).join();
        streamStore.setState(scope, testStream, State.ACTIVE, null, executor).join();
        assertEquals(firstEpochInOldFormat, streamStore.getEpoch(scope, testStream, 0, null, executor).join());

        ArrayList<Map.Entry<Double, Double>> newRange = new ArrayList<>();
        newRange.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        // start with manual scale
        ScaleOpEvent event = new ScaleOpEvent(scope, testStream, Lists.newArrayList(0L),
                newRange, true, System.currentTimeMillis(), System.currentTimeMillis());
        streamStore.submitScale(scope, testStream, Lists.newArrayList(0L),
                new ArrayList<>(newRange), System.currentTimeMillis(), null, null, executor).join();

        // perform scaling
        scaleRequestHandler.execute(event).join();
        assertEquals(State.ACTIVE, streamStore.getState(scope, testStream, true, null, executor).join());
        assertEquals(1, streamStore.getActiveEpoch(scope, testStream, null, true, executor).join().getEpoch());
    }
}
