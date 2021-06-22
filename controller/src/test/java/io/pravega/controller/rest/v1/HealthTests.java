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
package io.pravega.controller.rest.v1;

//import io.pravega.client.ClientConfig;
//import io.pravega.client.connection.impl.ConnectionFactory;
//import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.controller.server.rest.resources.HealthImpl;
import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.shared.rest.RESTServer;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.HealthDetails;
import io.pravega.controller.server.rest.generated.model.HealthResult;
import io.pravega.controller.server.rest.generated.model.HealthStatus;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.health.Health;
//
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import io.pravega.shared.rest.security.AuthHandlerManager;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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

import static org.junit.Assert.assertEquals;

@Slf4j
public class HealthTests {

        private static final String HOST = "localhost";

        private static final String IMPLICIT_INDICATOR = "implicit-health-child";

        private static final String IMPLICIT_COMPONENT = "health-component";

        private static final int INTERVAL = 1000;
        @Rule
        public final Timeout globalTimeout = new Timeout(10 * INTERVAL, TimeUnit.MILLISECONDS);

        private RESTServerConfig serverConfig;
        private RESTServer restServer;
        private Client client;
        private  HealthServiceManager healthServiceManager;

        @Before
        public void setup() throws Exception {
            serverConfig = getServerConfig();
            healthServiceManager = new HealthServiceManager(Duration.ofSeconds(1));
            AuthHandlerManager authHandlerManager = new AuthHandlerManager(serverConfig);
            restServer = new RESTServer(serverConfig,
                    Set.of(new HealthImpl(null, healthServiceManager.getEndpoint()), new PingImpl()));

            healthServiceManager.start();
            restServer.startAsync();
            restServer.awaitRunning();
            client = createJerseyClient();
            //healthServiceManager.getRoot().register(new StaticHealthyIndicator(IMPLICIT_INDICATOR));
        }

        @Test
        public void test() {
            URI streamResourceURI = UriBuilder.fromPath("//localhost:" + serverConfig.getPort() + "/ping")
                    .scheme(getURLScheme()).build();
            Response response = client.target(streamResourceURI).request().buildGet().invoke();
            assertEquals(200, response.getStatus());
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

    //private RESTServerConfig serverConfig;
    //private RESTServer restServer;
    //private Client client;


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
    public void testHealthNoContributors() {
        URI path = getURI(String.format("/v1/health/%s", IMPLICIT_COMPONENT));
        URI streamResourceURI = UriBuilder.fromUri(path)
                .scheme(getURLScheme()).build();

        Response response = client.target(streamResourceURI).request().buildGet().invoke();
        HealthResult healthResult = response.readEntity(HealthResult.class);
        response.close();

        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals("Health Status should be 'UNKNOWN'", HealthStatus.UNKNOWN, healthResult.getStatus());
    }

    //@Test
    //public void test() throws InterruptedException {
    //    URI streamResourceURI = UriBuilder.fromPath("//" + HOST + ":" + serverConfig.getPort() + "/ping")
    //            .scheme(getURLScheme()).build();
    //    //while (true) {
    //    //    Thread.sleep(1000);
    //    //}

    //    Response response = client.target(streamResourceURI).request().buildGet().invoke();
    //    assertEquals(200, response.getStatus());
    //}

    //@Test
    //public void testHealth()  {
    //    // Register the HealthIndicator.
    //    healthServiceManager.getRoot().register(new StaticHealthyIndicator());

    //    URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
    //            .scheme(getURLScheme()).build();
    //    Response response = client.target(streamResourceURI).request().buildGet().invoke();
    //    HealthResult healthResult = response.readEntity(HealthResult.class);

    //    Assert.assertEquals(200, response.getStatus());
    //    Assert.assertEquals("HealthService should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
    //    // Details not requested, so children will not be populated.
    //    Assert.assertTrue("HealthService should not provide children.", healthResult.getChildren().isEmpty());

    //    // Test *with* details.
    //    streamResourceURI = UriBuilder.fromUri(getURI("/v1/health"))
    //            .scheme(getURLScheme()).queryParam("details", true).build();
    //    response = client.target(streamResourceURI).request().buildGet().invoke();
    //    healthResult = response.readEntity(HealthResult.class);
    //    boolean found = false;
    //    for (HealthResult child : healthResult.getChildren()) {
    //        found |= !child.getDetails().isEmpty();
    //    }
    //    Assert.assertTrue("HealthService should provide details.", found);
    //}

    //@Test
    //public void testContributorHealth() {
    //    // Register the HealthIndicator.
    //    StaticHealthyIndicator indicator = new StaticHealthyIndicator();
    //    healthServiceManager.getRoot().register(indicator);

    //    URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", indicator.getName())))
    //            .scheme(getURLScheme()).build();
    //    log.info("{}", streamResourceURI);
    //    Response response = client.target(streamResourceURI).request().buildGet().invoke();
    //    HealthResult healthResult = response.readEntity(HealthResult.class);
    //    Assert.assertEquals(200, response.getStatus());
    //    Assert.assertEquals("The HealthIndicator should maintain an 'UP' Status.", HealthStatus.UP, healthResult.getStatus());
    //    Assert.assertTrue("The HealthIndicator should not provide details.", healthResult.getDetails().isEmpty());
    //}

    // Be mindful that the results will be dependent on the StatusAggregatorRule used.
    @Test
    public void testStatus() throws Exception {

        URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
       assertStatus(streamResourceURI, HealthStatus.UP);

        // Start with a HealthyIndicator.
        StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
        healthServiceManager.getRoot().register(healthyIndicator);

       streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.UP);

        // Adding an unhealthy indicator should change the Status.
        StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
        healthServiceManager.getRoot().register(failingIndicator);

        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);

        // Make sure that even though we have a majority of healthy reports, we still are considered failing.
        healthServiceManager.getRoot().register(new StaticHealthyIndicator("sample-healthy-indicator-two"));
        streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/status"))
                .scheme(getURLScheme()).build();
        assertStatus(streamResourceURI, HealthStatus.DOWN);
    }

    //// Service Readiness is in essence a proxy for the Status as a HealthComponent does not provide its own
    //// `doHealthCheck` logic.
    //@Test
    //public void testReadiness()  {
    //    // Start with a HealthyIndicator.
    //    StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
    //    healthServiceManager.getRoot().register(healthyIndicator);

    //    URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
    //            .scheme(getURLScheme()).build();
    //    assertAliveOrReady(streamResourceURI, true);

    //    // Adding an unhealthy indicator should change the readiness.
    //    StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
    //    healthServiceManager.getRoot().register(failingIndicator);

    //    streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/readiness"))
    //            .scheme(getURLScheme()).build();
    //    assertAliveOrReady(streamResourceURI, false);
    //}

    //@Test
    //public void testLiveness()  {
    //    // Start with a HealthyIndicator.
    //    StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
    //    healthServiceManager.getRoot().register(healthyIndicator);

    //    URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
    //            .scheme(getURLScheme()).build();
    //    assertAliveOrReady(streamResourceURI, true);

    //    // Adding an unhealthy indicator should change the readiness.
    //    StaticFailingIndicator failingIndicator = new StaticFailingIndicator();
    //    healthServiceManager.getRoot().register(failingIndicator);

    //    streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/liveness"))
    //            .scheme(getURLScheme()).build();
    //    assertAliveOrReady(streamResourceURI, false);
    //}

    //@Test
    //public void testContributorDependencies() {
    //    // Start with a HealthyIndicator.
    //    StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
    //    healthServiceManager.getRoot().register(healthyIndicator);
    //    URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/components/%s", healthyIndicator.getName())))
    //            .scheme(getURLScheme()).build();
    //}

    //@Test
    //public void testDetails()  {
    //    // Register the HealthIndicator.
    //    healthServiceManager.getRoot().register(new StaticHealthyIndicator());
    //    URI streamResourceURI = UriBuilder.fromUri(getURI("/v1/health/details"))
    //            .scheme(getURLScheme()).build();
    //    Response response = client.target(streamResourceURI).request().buildGet().invoke();
    //    HealthDetails details = response.readEntity(HealthDetails.class);
    //    Assert.assertTrue("HealthService does not provide details itself.", details.isEmpty());
    //}

    //@Test
    //public void testContributorDetails() {
    //    // Register the HealthIndicator.
    //    StaticHealthyIndicator healthyIndicator = new StaticHealthyIndicator();
    //    healthServiceManager.getRoot().register(healthyIndicator);
    //    URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/details/%s", healthyIndicator.getName())))
    //            .scheme(getURLScheme()).build();
    //    HealthDetails expected = new HealthDetails();
    //    expected.put(StaticHealthyIndicator.DETAILS_KEY, StaticHealthyIndicator.DETAILS_VAL);
    //    assertDetails(streamResourceURI, expected);
    //}

    //@Test
    //public void testContributorNotExists() {
    //    String unknown = "unknown-indicator";
    //    // Register the HealthIndicator.
    //    URI streamResourceURI = UriBuilder.fromUri(getURI(String.format("/v1/health/%s", unknown)))
    //            .scheme(getURLScheme()).build();
    //    Response response = client.target(streamResourceURI).request().buildGet().invoke();
    //    Assert.assertEquals(404, response.getStatus());
    //}

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

    private void assertStatus(URI uri, HealthStatus expected) throws Exception {
        AssertExtensions.assertEventuallyEquals("Expected a successful (200) response code.",
                200,
                () -> client.target(uri).request().buildGet().invoke().getStatus(),
                INTERVAL,
                INTERVAL * 3);
        AssertExtensions.assertEventuallyEquals(String.format("Expected a '%s' HealthStatus.", expected.toString()),
                expected,
                () -> client.target(uri).request().buildGet().invoke().readEntity(HealthStatus.class),
                INTERVAL,
                INTERVAL * 3);
    }

    private void assertAliveOrReady(URI uri, boolean expected) {
        Response response = client.target(uri).request().buildGet().invoke();
        boolean ready = response.readEntity(Boolean.class);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertEquals(String.format("The StaticHealthyIndicator should be %s.", expected ? "alive/ready" : "not alive/ready"),
                expected,
                ready);
        response.close();
    }

    private static class StaticHealthyIndicator extends AbstractHealthContributor {
        public static final String DETAILS_KEY = "static-indicator-details-key";

        public static final String DETAILS_VAL = "static-indicator-details-value";


        public StaticHealthyIndicator() {
            super("static-healthy-indicator");
        }

        public StaticHealthyIndicator(String name) {
            super(name);
        }

        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.UP;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }

    private static class StaticFailingIndicator extends AbstractHealthContributor {

        public static final String DETAILS_KEY = "static-failing-indicator-details-key";

        public static final String DETAILS_VAL = "static-failing-indicator-details-value";

        public StaticFailingIndicator() {
            super("static-failing-indicator");
        }

        public StaticFailingIndicator(String name) {
            super(name);
        }

        public Status doHealthCheck(Health.HealthBuilder builder) {
            Status status = Status.DOWN;
            Map<String, Object> details = new HashMap<>();
            details.put(DETAILS_KEY, DETAILS_VAL);
            builder.status(status).details(details);
            return status;
        }
    }
}