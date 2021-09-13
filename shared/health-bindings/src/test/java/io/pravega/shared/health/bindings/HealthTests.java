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
package io.pravega.shared.health.bindings;

import io.pravega.shared.health.bindings.generated.model.HealthDetails;
import io.pravega.shared.health.bindings.generated.model.HealthResult;
import io.pravega.shared.health.bindings.generated.model.HealthStatus;
import io.pravega.shared.health.bindings.resources.HealthImpl;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import io.pravega.shared.rest.RESTServer;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.Timeout;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HealthTests {

    private static final String HOST = "localhost";

    private static final int INTERVAL = 1000;
    @Rule
    public final Timeout globalTimeout = new Timeout(20 * INTERVAL, TimeUnit.MILLISECONDS);

    private RESTServerConfig serverConfig;
    private RESTServer restServer;
    private Client client;
    private HealthServiceManager healthServiceManager;

    @Before
    public void setup() throws Exception {
        serverConfig = getServerConfig();
        healthServiceManager = new HealthServiceManager(Duration.ofMillis(100));
        restServer = new RESTServer(serverConfig,
                Set.of(new HealthImpl(null, healthServiceManager.getEndpoint())));

        healthServiceManager.start();
        restServer.startAsync();
        restServer.awaitRunning();
        client = createJerseyClient();
    }

    protected Client createJerseyClient() {
        return ClientBuilder.newClient();
    }

    RESTServerConfig getServerConfig() {
        return RESTServerConfigImpl.builder().host(HOST).port(TestUtils.getAvailableListenPort())
                .build();
    }

    protected String getURLScheme() {
        return "http";
    }

    @After
    public void tearDown() {
        healthServiceManager.close();
        client.close();
        restServer.stopAsync();
        restServer.awaitTerminated();
    }


    protected URI getURI(String path) {
        return UriBuilder.fromPath(path)
                .scheme(getURLScheme())
                .host(serverConfig.getHost())
                .port(serverConfig.getPort())
                .build();
    }

    @Test
    public void testHealth() throws Exception {
        // Register the HealthIndicator.
        healthServiceManager.register(new StaticHealthyContributor());

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
                .scheme(getURLScheme()).build();

        URI serviceStatusURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(serviceStatusURI, HealthStatus.UP);
        URI contributorStatusURI = UriBuilder.fromUri(getURI("/v1/health/status/" + StaticHealthyContributor.NAME))
                .scheme(getURLScheme()).build();
        // Assert the registered contributor has been initialized.
        assertStatus(contributorStatusURI, HealthStatus.UP);

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        boolean found = false;
        for (val child : healthResult.getChildren().entrySet()) {
            found |= !child.getValue().getDetails().isEmpty();
        }
        Assert.assertTrue("HealthService should provide details.", found);
    }

    @Test
    public void testContributorHealth() throws Exception {
        // Register the HealthIndicator.
        StaticHealthyContributor contributor = new StaticHealthyContributor();
        healthServiceManager.register(contributor);

        // Wait for contributor initialization.
        URI statusURI = UriBuilder.fromUri(getURI(String.format("/v1/health/status/%s", contributor.getName())))
                .scheme(getURLScheme()).build();
        assertStatus(statusURI, HealthStatus.UP);

        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", contributor.getName())))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);

        Assert.assertEquals("Unexpected number of child contributors.",
            0,
            healthResult.getChildren().size());

        Assert.assertEquals("Child HealthContributor failed to report its detail entry.",
                1,
                healthResult.getDetails().size());
    }

    @Test
    public void testStatus() throws Exception {

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
       assertStatus(streamResourceURI, HealthStatus.UP);

        // Start with a HealthyIndicator.
        StaticHealthyContributor healthyIndicator = new StaticHealthyContributor();
        healthServiceManager.register(healthyIndicator);

       streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.UP);

        // Adding an unhealthy indicator should change the Status.
        StaticFailingContributor failingIndicator = new StaticFailingContributor();
        healthServiceManager.register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);

        // Make sure that even though we have a majority of healthy reports, we still are considered failing.
        healthServiceManager.register(new StaticHealthyContributor("sample-healthy-indicator-two"));
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);
    }

    @Test
    public void testReadiness() throws Exception {
        // Start with a HealthyContributor.
        StaticHealthyContributor healthyIndicator = new StaticHealthyContributor();
        healthServiceManager.register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy contributor should change the readiness status.
        StaticFailingContributor failingContributor = new StaticFailingContributor();
        healthServiceManager.register(failingContributor);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testLiveness() throws Exception {
        // Start with a HealthyContributor.
        StaticHealthyContributor healthyIndicator = new StaticHealthyContributor();
        healthServiceManager.register(healthyIndicator);

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, true);

        // Adding an unhealthy contributor should change the readiness.
        StaticFailingContributor failingIndicator = new StaticFailingContributor();
        healthServiceManager.register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
                .scheme(getURLScheme()).build();
        assertAliveOrReady(streamResourceURI, false);
    }

    @Test
    public void testDetails()  {
        // Register the HealthIndicator.
        healthServiceManager.register(new StaticHealthyContributor());
        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/details"))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthDetails details = response.readEntity(HealthDetails.class);
        Assert.assertTrue("HealthService does not provide details itself.", details.isEmpty());
    }

    @Test
    public void testContributorDetails() throws Exception {
        // Register the HealthIndicator.
        StaticHealthyContributor healthyIndicator = new StaticHealthyContributor();
        healthServiceManager.register(healthyIndicator);

        URI statusURI = UriBuilder.fromUri(getURI("/v1/health/status/" + StaticHealthyContributor.NAME))
                .scheme(getURLScheme()).build();
        assertStatus(statusURI, HealthStatus.UP);

        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/details/%s", healthyIndicator.getName())))
                .scheme(getURLScheme()).build();
        HealthDetails expected = new HealthDetails();
        expected.put(StaticHealthyContributor.DETAILS_KEY, StaticHealthyContributor.DETAILS_VAL);
        assertDetails(streamResourceURI, expected);
    }

    @Test
    public void testContributorNotExists() throws Exception {
        String unknown = "unknown-contributor";
        // Wait for the health updater to have updated at least one time.
        URI statusURI = UriBuilder.fromUri(getURI("/v1/health/status")).scheme(getURLScheme()).build();
        assertStatus(statusURI, HealthStatus.UP);

        URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", unknown)))
                .scheme(getURLScheme()).build();
        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        Assert.assertEquals(404, response.getStatus());
    }

    private void assertDetails(URI uri, HealthDetails expected) throws Exception {

        AssertExtensions.assertEventuallyEquals("Expected a successful (200) response code.",
                200,
                () -> client.target(uri).request().buildGet().invoke().getStatus(),
                INTERVAL,
                INTERVAL * 3);

        AssertExtensions.assertEventuallyEquals(String.format("Expected %d HealthDetails.", expected.size()),
                expected.size(),
                () -> client.target(uri).request().buildGet().invoke().readEntity(HealthDetails.class).size(),
                INTERVAL,
                INTERVAL * 3);

        HealthDetails details = client.target(uri).request().buildGet().invoke().readEntity(HealthDetails.class);

        details.forEach((key, val) -> {
            if (!expected.get(key).equals(val)) {
                Assert.assertEquals("Unexpected value for detail entry.", expected.get(key), val);
            }
        });
    }

    private void assertResponseSuccess(URI uri) throws Exception {
        AssertExtensions.assertEventuallyEquals("Expected a successful (200) response code.",
                200,
                () -> client.target(uri).request().buildGet().invoke().getStatus(),
                INTERVAL,
                INTERVAL * 3);
    }

    private void assertStatus(URI uri, HealthStatus expected) throws Exception {
        assertResponseSuccess(uri);
        AssertExtensions.assertEventuallyEquals(String.format("Expected a '%s' HealthStatus.", expected.toString()),
                expected,
                () -> client.target(uri).request().buildGet().invoke().readEntity(HealthStatus.class),
                INTERVAL,
                INTERVAL * 3);
    }

    private void assertAliveOrReady(URI uri, boolean expected) throws Exception {
        assertResponseSuccess(uri);
        AssertExtensions.assertEventuallyEquals(String.format("The StaticHealthyContributor was expected to be %s", expected ? "alive/ready" : "dead/not-ready"),
                expected,
                () -> client.target(uri).request().buildGet().invoke().readEntity(Boolean.class),
                INTERVAL,
                INTERVAL * 3);
    }

    private static class StaticHealthyContributor extends AbstractHealthContributor {
        public static final String DETAILS_KEY = "static-contributor-details-key";

        public static final String NAME = "static-healthy-contributor";

        public static final String DETAILS_VAL = "static-contributor-details-value";

        public StaticHealthyContributor() {
            super(NAME);
        }

        public StaticHealthyContributor(String name) {
            super(name);
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }

    private static class StaticFailingContributor extends AbstractHealthContributor {

        public static final String DETAILS_KEY = "static-failing-contributor-details-key";

        public static final String DETAILS_VAL = "static-failing-contributor-details-value";

        public StaticFailingContributor() {
            super("static-contributor-indicator");
        }

        @Override
        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.DOWN;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }
}