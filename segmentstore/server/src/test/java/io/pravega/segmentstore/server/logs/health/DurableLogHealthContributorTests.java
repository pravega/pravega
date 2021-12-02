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
package io.pravega.segmentstore.server.logs.health;

import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.logs.ContainerSetup;
import io.pravega.segmentstore.server.logs.DurableLog;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class DurableLogHealthContributorTests {

    private DurableLog log;
    private ContainerSetup container;
    private ScheduledExecutorService service;

    @Before
    public void setup() {
        service = ExecutorServiceHelpers.newScheduledThreadPool(2, "DurableLogHealthContributorTests");
        container = ContainerSetup.builder()
                .executorService(service)
                .checkPointMinCommitCount(10)
                .containerId(12345)
                .maxAppendSize(8 * 1024)
                .readIndexConfig(ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build())
                .build();
        log = container.createDurableLog();
        log.startAsync().awaitRunning();
    }

    @After
    public void tearDown() {
        log.stopAsync().awaitTerminated();
        log.close();
        container.close();
        service.shutdown();
    }

    @Test
    public void testHealthCheck() {
        DurableLogHealthContributor contributor = new DurableLogHealthContributor("log", log);

        Map<String, Object> details = contributor.getHealthSnapshot().getDetails();

        Assert.assertEquals(details.get("State"), Service.State.RUNNING);
        Assert.assertEquals(details.get("IsOffline"), false);
    }

}
