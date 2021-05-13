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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

import io.pravega.shared.health.TestHealthIndicators.SampleHealthyIndicator;

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
        // Except its root.
        Assert.assertEquals("The HealthService should not maintain any references to HealthContributors.",
                1,
                service.getComponents().size());
        // Except its root.
        Assert.assertEquals("The ContributorRegistry should not maintain any references to HealthContributors",
                1,
                service.getRegistry().getContributors().size());
    }

    /**
     * Tests that the {@link HealthService} is able to provide {@link Health} results for both a specific {@link HealthIndicator}
     * and the service itself ({@link ContributorRegistry#getRootContributor()}.
     */
    @Test
    public void testHealth() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        Health health = service.getEndpoint().getHealth(indicator.getName());
        Assert.assertTrue("Status of the default/root component is expected to be 'UP'", health.getStatus() == Status.UP);

        health = service.getEndpoint().getHealth(true);
        Assert.assertEquals("There should be exactly one child (SimpleIndicator)", health.getChildren().size(), 1);
    }

    /**
     * Tests that a null value is returned when an invalid {@link HealthContributor} id/name is provided.
     */
    @Test
    public void testHealthInvalidName() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);
        Assert.assertNull(service.getEndpoint().getHealth("unknown-indicator-name"));
    }


    /**
     * Tests that the {@link HealthService} is properly able to distinguish between those {@link HealthContributor}
     * which are {@link io.pravega.shared.health.impl.HealthComponent} and those that are {@link HealthIndicator}.
     *
     */
    @Test
    public void testComponents() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        Collection<String> components = service.getComponents();
        // Only the 'ROOT' HealthComponent should be registered.
        Assert.assertEquals("No non-root components have been defined.", service.getComponents().size(), 1);
        // Assert that it is indeed the 'ROOT'
        Assert.assertEquals("Expected the name of the returned component to match the ROOT's given name.",
                ROOT_NAME,
                components.stream().findFirst().get());
    }

    /**
     * Tests that all the {@link HealthContributor} objects that are used to arrive at a {@link Health} decision for another
     * {@link HealthContributor} are provided.
     *
     * Note: A 'dependency' is any sub-class of {@link HealthContributor}.
     */
    @Test
    public void testDependencies() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        // Test 'service level' endpoint.
        Collection<String> dependencies = service.getEndpoint().getDependencies();
        Assert.assertEquals("There should be exactly one dependency.", 1, dependencies.size());
        Assert.assertEquals("That dependency should be named 'sample-healthy-indicator'.", indicator.getName(), dependencies.stream().findFirst().get());
        // Test Contributor level endpoint.
        dependencies = service.getEndpoint().getDependencies(indicator.getName());
        Assert.assertEquals("The HealthIndicator should not have any dependencies.", 0, dependencies.size());
    }

    /**
     * Tests that when specified a {@link Health} result will return any details information belonging to that
     * {@link HealthContributor} or it's dependencies.
     *
     * A {@link io.pravega.shared.health.impl.HealthComponent} does not directly supply any details information,
     * but any dependency (that is of type {@link HealthIndicator}) that it has should supply them.
     */
    @Test
    public void testDetailsEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        Health health = service.getEndpoint().getHealth(true);
        Assert.assertTrue("There should be at least one child (SimpleIndicator)", health.getChildren().size() >= 1);

        // Tests that the Details object(s) are reachable via the 'service level' endpoint.
        Health sample = health.getChildren().stream().findFirst().get();
        Assert.assertEquals("There should be one details entry provided by the SimpleIndicator.", 1, sample.getDetails().size());
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                SampleHealthyIndicator.DETAILS_KEY,
                sample.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                SampleHealthyIndicator.DETAILS_VAL,
                sample.getDetails().entrySet().stream().findFirst().get().getValue());

        // Directly query the HealthContributor endpoint.
        health = service.getEndpoint().getHealth(indicator.getName(), true);
        Assert.assertEquals(String.format("Key should equal \"%s\"", SampleHealthyIndicator.DETAILS_KEY),
                SampleHealthyIndicator.DETAILS_KEY,
                health.getDetails().keySet().stream().findFirst().get());
        Assert.assertEquals(String.format("Value should equal \"%s\"", SampleHealthyIndicator.DETAILS_VAL),
                SampleHealthyIndicator.DETAILS_VAL,
                health.getDetails().get(SampleHealthyIndicator.DETAILS_KEY));
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its {@link Status}
     * information.
     */
    @Test
    public void testStatusEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        Status status = service.getEndpoint().getStatus();
        // Test the 'service level' endpoint.
        Assert.assertEquals("Status should be UP.", Status.UP, status);
        // Test the Contributor level endpoint.
        status = service.getEndpoint().getStatus(indicator.getName());
        Assert.assertEquals("Status should be UP.", Status.UP, status);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its liveness information.
     */
    @Test
    public void testLivenessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        boolean alive = service.getEndpoint().isAlive();
        Assert.assertEquals("The HealthService should produce an 'alive' result.", true, alive);
        alive = service.getEndpoint().isAlive(indicator.getName());
        Assert.assertEquals("The HealthIndicator should produce an 'alive' result.", true, alive);
    }

    /**
     * Test that both the {@link HealthService} and a {@link HealthContributor} can be queried for its readiness information.
     */
    @Test
    public void testReadinessEndpoints() {
        SampleHealthyIndicator indicator = new SampleHealthyIndicator();
        service.getRegistry().register(indicator);

        boolean ready = service.getEndpoint().isReady();
        Assert.assertEquals("The HealthService should produce a 'ready' result.", true, ready);
        ready = service.getEndpoint().isReady(indicator.getName());
        Assert.assertEquals("The SampleIndicator should produce a 'ready' result.", true, ready);
    }
}
