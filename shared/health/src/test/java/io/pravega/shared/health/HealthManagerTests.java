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

import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthContributors.HealthyContributor;

import static io.pravega.shared.health.TestHealthContributors.awaitHealthContributor;

/**
 * The {@link HealthManagerTests} encapsulates much of the same processes that {@link HealthEndpoint} performs, so
 * an explicit test class for the former is skipped.
 */
@Slf4j
public class HealthManagerTests {

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

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

    /**
     * Tests that the {@link HealthServiceManager} is able to provide {@link Health} results for both a specific {@link HealthContributor}
     * and the service itself ({@link HealthServiceManager}.
     */
    @Test
    public void testHealth() throws Exception {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor();
        service.register(contributor);

        awaitHealthContributor(service, contributor.getName());
        Assert.assertNotNull(service.getEndpoint().getHealth(contributor.getName()));
        Assert.assertNotNull(service.getEndpoint().getHealth());
        Assert.assertEquals("Status of the default/root component is expected to be 'UP'", Status.UP, service.getEndpoint().getStatus());
        Assert.assertEquals("There should be exactly one child (SimpleIndicator)", 1, service.getEndpoint().getHealth().getChildren().size());
    }

    /**
     * Tests that a null value is returned when an invalid {@link HealthContributor} id/name is provided.
     */
    @Test
    public void testHealthInvalidName() {
        AssertExtensions.assertThrows("An exception should be thrown given an unregistered contributor.",
                () -> TestHealthContributors.awaitHealthContributor(service, "unknown-contributor-name"),
                e -> e instanceof TimeoutException);
    }

    /**
     * Tests that when specified a {@link Health} result will return any details information belonging to that
     * {@link HealthContributor} or it's dependencies.
     */
    @Test
    public void testDetailsEndpoints() throws TimeoutException {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.register(contributor);

        // Wait for the health result to be picked up by the HealthServiceUpdater.
        awaitHealthContributor(service, contributor.getName());
        Health health = service.getEndpoint().getHealth();
        Assert.assertEquals("There should be at least one child contributor", 1, health.getChildren().size());

        // Tests that the Details object(s) are reachable via the 'service level' endpoint.
        Health sample = health.getChildren().entrySet().stream().findFirst().get().getValue();
        Assert.assertEquals("There should be one details entry provided by the SimpleIndicator.", 1, sample.getDetails().size());
        Assert.assertEquals(String.format("Key should equal \"%s\"", HealthyContributor.DETAILS_KEY),
                HealthyContributor.DETAILS_KEY,
                sample.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", HealthyContributor.DETAILS_VAL),
                HealthyContributor.DETAILS_VAL,
                sample.getDetails().entrySet().stream().findFirst().get().getValue());

        // Directly query the HealthContributor endpoint.
        health = service.getEndpoint().getHealth(contributor.getName());
        Assert.assertEquals(String.format("Key should equal \"%s\"", HealthyContributor.DETAILS_KEY),
                HealthyContributor.DETAILS_KEY,
                health.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", HealthyContributor.DETAILS_VAL),
                HealthyContributor.DETAILS_VAL,
                health.getDetails().get(HealthyContributor.DETAILS_KEY));
    }

    /**
     * Test that both the {@link HealthServiceManager} and a {@link HealthContributor} can be queried for its {@link Status}
     * information.
     */
    @Test
    public void testStatusEndpoints() throws Exception {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.register(contributor);

        awaitHealthContributor(service, contributor.getName());
        // Test the 'service level' endpoint.
        Assert.assertEquals("Status should be UP", Status.UP, service.getEndpoint().getStatus());
        // Test the contributor level endpoint.
        Assert.assertEquals("Status should be UP", Status.UP, service.getEndpoint().getStatus(contributor.getName()));
    }

    /**
     * Test that both the {@link HealthServiceManager} and a {@link HealthContributor} can be queried for its liveness information.
     */
    @Test
    public void testLivenessEndpoints() throws Exception {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.register(contributor);

        awaitHealthContributor(service, contributor.getName());
        Assert.assertEquals("The HealthServiceManager should produce an 'alive' result.",
                true, service.getEndpoint().isAlive());

        Assert.assertEquals("The HealthContributor with id should produce an 'alive' result.",
                true, service.getEndpoint().isAlive(contributor.getName()));
    }

    /**
     * Test that both the {@link HealthServiceManager} and a {@link HealthContributor} can be queried for its readiness information.
     */
    @Test
    public void testReadinessEndpoints() throws TimeoutException {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.register(contributor);
        // Wait for the HealthServiceUpdater to update the Health state.
        awaitHealthContributor(service, contributor.getName());

        boolean ready = service.getEndpoint().isReady();
        Assert.assertEquals("The HealthServiceManager should produce a 'ready' result.", true, ready);
        ready = service.getEndpoint().isReady(contributor.getName());
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);
    }

}