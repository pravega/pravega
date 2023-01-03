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

package io.pravega.controller.store.index;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.PravegaZkCuratorResource;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryOneTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ZkHostIndexTest {

    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(RETRY_POLICY);
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    @Before
    public void setUp() throws Exception {

    }
    
    @After
    public void tearDown() throws IOException {
        ExecutorServiceHelpers.shutdown(executor);
    }
    
    @Test(timeout = 30000)
    public void testSync() {
        ZKHostIndex index = spy(new ZKHostIndex(PRAVEGA_ZK_CURATOR_RESOURCE.client, "/hostRequestIndex", executor));
        String hostId = "hostId";
        index.addEntity(hostId, "entity").join();
        List<String> entities = index.getEntities(hostId).join();
        verify(index, times(1)).sync(any());
        assertEquals(entities.size(), 1);

        Set<String> hosts = index.getHosts().join();
        verify(index, times(2)).sync(any());
        assertTrue(hosts.contains(hostId));
    }
}
