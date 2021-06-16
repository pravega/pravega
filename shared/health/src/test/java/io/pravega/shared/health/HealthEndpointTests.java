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
package io.pravega.shared.health;

import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.FailingContributor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class HealthEndpointTests {

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

     HealthServiceManager service;

    @Before
    public void before() {
        service = new HealthServiceManager(Duration.ofSeconds(1));
        service.getHealthServiceUpdater().startAsync();
        service.getHealthServiceUpdater().awaitRunning();
    }

    @After
    public void after() {
        service.getHealthServiceUpdater().stopAsync();
        service.getHealthServiceUpdater().awaitTerminated();
        service.close();
    }

    @Test
    public void testContributorNameResolution() throws Exception {
        @Cleanup
        HealthyContributor parent = new HealthyContributor("parent");
        @Cleanup
        FailingContributor first = new FailingContributor("child");
        parent.register(first);
        service.getRoot().register(parent);
        // Also register a contributor directly under the root.
        HealthyContributor second = new HealthyContributor("child");
        service.getRoot().register(second);

        // Wait for the HealthServiceUpdater to update the Health state.
        TestUtils.await(() -> service.getEndpoint().getHealth("parent/child") != null,
                (int) service.getHealthServiceUpdater().getInterval().toMillis(),
                service.getHealthServiceUpdater().getInterval().toMillis() * 2);
        TestUtils.await(() -> service.getEndpoint().getHealth("child") != null,
                (int) service.getHealthServiceUpdater().getInterval().toMillis(),
                service.getHealthServiceUpdater().getInterval().toMillis() * 2);
        // Now request the health objects from the endpoint and ensure the right child is returned.
        Assert.assertEquals(false, service.getEndpoint().getHealth("parent/child").getStatus().isAlive());
        Assert.assertEquals(true, service.getEndpoint().getHealth("child").getStatus().isAlive());

        // Assert that requesting a non-existent health contributor returns null.
        Assert.assertNull("Requesting a non-existent HealthContributor did not throw an exception.",
                service.getEndpoint().getHealth("unknown"));
    }
}
