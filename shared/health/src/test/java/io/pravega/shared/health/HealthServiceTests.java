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

import io.pravega.shared.health.impl.HealthEndpointImpl;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthContributors.HealthyContributor;

/**
 * The {@link HealthServiceTests} encapsulates much of the same processes that {@link HealthEndpoint} performs, so
 * an explicit test class for the former is skipped.
 */
@Slf4j
public class HealthServiceTests {


    private static final String ROOT_NAME = "root";

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    HealthService service;

    HealthServiceFactory factory;

    @Before
    @SneakyThrows
    public void before() {
        factory = new HealthServiceFactory();
        service = factory.createHealthService(ROOT_NAME);
    }

    @After
    public void after() {
        service.close();
        factory.close();
    }

    /**
     * Tests that the {@link HealthService} is able to provide {@link Health} results for both a specific {@link HealthContributor}
     * and the service itself ({@link HealthService}.
     */
    @Test
    public void testHealth() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor();
        service.getRoot().register(contributor);

        Health health = service.getEndpoint().getHealth(contributor.getName());
        Assert.assertTrue("Status of the default/root component is expected to be 'UP'", health.getStatus() == Status.UP);

        health = service.getEndpoint().getHealth();
        Assert.assertEquals("There should be exactly one child (SimpleIndicator)", health.getChildren().size(), 1);
    }

    /**
     * Tests that a null value is returned when an invalid {@link HealthContributor} id/name is provided.
     */
    @Test
    public void testHealthInvalidName() {
        AssertExtensions.assertThrows("An exception should be thrown given an unregisterd contributor.",
                () -> service.getEndpoint().getHealth("unknown-contributor-name"),
                ex -> ex instanceof HealthEndpointImpl.ContributorNotFoundException);
    }

    /**
     * Tests that when specified a {@link Health} result will return any details information belonging to that
     * {@link HealthContributor} or it's dependencies.
     *
     */
    @Test
    public void testDetailsEndpoints() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.getRoot().register(contributor);

        Health health = service.getEndpoint().getHealth();
        Assert.assertEquals("There should be at least one child contributor", 1, health.getChildren().size());

        // Tests that the Details object(s) are reachable via the 'service level' endpoint.
        Health sample = health.getChildren().stream().findFirst().get();
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
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its {@link Status}
     * information.
     */
    @Test
    public void testStatusEndpoints() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.getRoot().register(contributor);

        Status status = service.getEndpoint().getStatus();
        // Test the 'service level' endpoint.
        Assert.assertEquals("Status should be UP.", Status.UP, status);
        // Test the Contributor level endpoint.
        status = service.getEndpoint().getStatus(contributor.getName());
        Assert.assertEquals("Status should be UP.", Status.UP, status);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its liveness information.
     */
    @Test
    public void testLivenessEndpoints() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.getRoot().register(contributor);

        boolean alive = service.getEndpoint().isAlive();
        Assert.assertEquals("The HealthService should produce an 'alive' result.", true, alive);
        alive = service.getEndpoint().isAlive(contributor.getName());
        Assert.assertEquals("The HealthIndicator should produce an 'alive' result.", true, alive);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its readiness information.
     */
    @Test
    public void testReadinessEndpoints() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor("contributor");
        service.getRoot().register(contributor);

        boolean ready = service.getEndpoint().isReady();
        Assert.assertEquals("The HealthService should produce a 'ready' result.", true, ready);
        ready = service.getEndpoint().isReady(contributor.getName());
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);
    }
}
