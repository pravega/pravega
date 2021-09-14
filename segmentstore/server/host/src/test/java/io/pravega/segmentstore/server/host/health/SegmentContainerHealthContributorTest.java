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
package io.pravega.segmentstore.server.host.health;

import com.google.common.util.concurrent.Service;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test health contributor for SegmentContainer.
 */
public class SegmentContainerHealthContributorTest {
    SegmentContainer segmentContainer;
    SegmentContainerHealthContributor segmentContainerHealthContributor;

    @Before
    public void setup() {
        segmentContainer = mock(SegmentContainer.class);
        segmentContainerHealthContributor = new SegmentContainerHealthContributor(segmentContainer);
    }

    @After
    public void tearDown() {
        segmentContainer.close();
        segmentContainerHealthContributor.close();
    }

    /**
     * Check health of SegmentContainer with different states.
     */
    @Test
    public void testSegmentContainerHealth() {
        when(segmentContainer.state()).thenReturn(Service.State.NEW);
        Health.HealthBuilder builder = Health.builder().name(segmentContainerHealthContributor.getName());
        Status status = segmentContainerHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'NEW' Status.", Status.NEW, status);
        when(segmentContainer.state()).thenReturn(Service.State.STARTING);
        status = segmentContainerHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'STARTING' Status.", Status.STARTING, status);
        when(segmentContainer.state()).thenReturn(Service.State.RUNNING);
        status = segmentContainerHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, status);
        when(segmentContainer.state()).thenReturn(Service.State.TERMINATED);
        status = segmentContainerHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'DOWN' Status.", Status.DOWN, status);
    }
}
