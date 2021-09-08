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

import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test health contributor for SegmentContainerRegistry.
 */
public class SegmentContainerRegistryHealthContributorTest {
    SegmentContainerRegistry segmentContainerRegistry;
    SegmentContainerRegistryHealthContributor segmentContainerRegistryHealthContributor;

    @Before
    public void setup() {
        segmentContainerRegistry = mock(SegmentContainerRegistry.class);
        segmentContainerRegistryHealthContributor = new SegmentContainerRegistryHealthContributor(segmentContainerRegistry);
        when(segmentContainerRegistry.getContainers()).thenReturn(new ArrayList<SegmentContainer>());
    }

    @After
    public void tearDown() {
        segmentContainerRegistry.close();
        segmentContainerRegistryHealthContributor.close();
    }

    /**
     * Check health of SegmentContainerRegistry with different states.
     */
    @Test
    public void testSegmentContainerHealth() {
        when(segmentContainerRegistry.isClosed()).thenReturn(true);
        Health.HealthBuilder builder = Health.builder().name(segmentContainerRegistryHealthContributor.getName());
        Status health = segmentContainerRegistryHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'DOWN' Status.", Status.DOWN, health);
        when(segmentContainerRegistry.isClosed()).thenReturn(false);
        health = segmentContainerRegistryHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, health);
    }
}
