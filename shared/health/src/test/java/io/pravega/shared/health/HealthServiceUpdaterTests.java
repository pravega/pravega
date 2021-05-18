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

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.FailingContributor;

import io.pravega.test.common.AssertExtensions;

@Slf4j
public class HealthServiceUpdaterTests {

    HealthService service;

    HealthServiceUpdater healthServiceUpdater;

    HealthServiceFactory factory;
    @Before
    public void before() {
        factory = new HealthServiceFactory();
        service = factory.createHealthService("health-service");
        service.getHealthServiceUpdater().startAsync();
        healthServiceUpdater = service.getHealthServiceUpdater();
        healthServiceUpdater.awaitRunning();
    }

    @After
    public void after() {
        factory.close();
        service.close();
        healthServiceUpdater.stopAsync();
        healthServiceUpdater.awaitTerminated();
    }

    @Test
    public void testIsRunningAfterServiceInitialization() {
        Assert.assertTrue(healthServiceUpdater.isRunning());
    }

    @Test
    public void testServiceUpdaterProperlyUpdates() throws Exception {
        service.getRoot().add(new HealthyContributor());
        // First Update.
        assertHealthServiceStatus(Status.UP);
        // We register an indicator that will return a failing result, so the next health check should contain a 'DOWN' Status.
        service.getRoot().add(new FailingContributor());
        assertHealthServiceStatus(Status.DOWN);
    }

    private void assertHealthServiceStatus(Status expected) throws Exception {
            AssertExtensions.assertEventuallyEquals(expected,
                    () -> healthServiceUpdater.getLatestHealth().getStatus(), healthServiceUpdater.getInterval().toMillis() + 1);
    }
}
