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

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.FailingContributor;

import java.time.Duration;

@Slf4j
public class HealthServiceUpdaterTests {

    HealthServiceManager service;

    @Before
    public void before() {
        service = new HealthServiceManager(Duration.ofMillis(100));
        service.start();
    }

    @After
    public void after() {
        service.close();
    }

    @Test
    public void testServiceUpdaterProperlyUpdates() throws Exception {
        @Cleanup
        HealthContributor contributor = new HealthyContributor("contributor");
        service.register(contributor);

        TestHealthContributors.awaitHealthContributor(service, service.getName());
        Health health = service.getEndpoint().getHealth();
        Assert.assertEquals(Status.UP, health.getStatus());
        contributor.close();

        Assert.assertEquals("Closed contributor should no longer be listed as a child.",
                0,
                service.getHealthSnapshot().getChildren().size());
        // We register an indicator that will return a failing result, so the next health check should contain a 'DOWN' Status.
        contributor = new FailingContributor("failing");
        service.register(contributor);
        TestHealthContributors.awaitHealthContributor(service, contributor.getName());

        health = service.getEndpoint().getHealth();
        Assert.assertEquals(Status.DOWN, health.getStatus());
    }
}
