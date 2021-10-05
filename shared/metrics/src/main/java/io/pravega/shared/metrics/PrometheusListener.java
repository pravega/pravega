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
package io.pravega.shared.metrics;

import com.google.common.util.concurrent.AbstractIdleService;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.pravega.common.LoggerHelpers;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A service that responds to Prometheus scrape requests, serving metrics
 * from a PrometheusMeterRegistry. The listening port and address can be
 * configured. Scraping clients should fetch from the pseudo standard
 * "/metrics" route path.
 */
@Slf4j
public class PrometheusListener extends AbstractIdleService {
    private final URI baseUri;
    private final PrometheusMeterRegistry registry;
    private final PrometheusResource resource;
    private final ResourceConfig resourceConfig;
    private final String objectId;
    private HttpServer httpServer;

    /**
     * @param address Local address for the network listener, or "0.0.0.0"
     *                to listen on all addresses.
     * @param port Local port for the network listener.
     * @param registry The micrometer registry containing metrics to serve.
     */
    PrometheusListener(String address, int port, PrometheusMeterRegistry registry) {
        this.objectId = "PrometheusListener";
        this.baseUri = URI.create("http://" + address + ":" + port + "/");
        this.registry = registry;
        this.resource = new PrometheusResource(registry);

        this.resourceConfig = ResourceConfig.forApplication(new Application() {
            @Override
            public Set<Object> getSingletons() {
                return Set.of(resource);
            }
        });
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        try {
            log.info("Starting prometheus scrape server listening on: {}", this.baseUri);
            httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, resourceConfig, true);
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        try {
            log.info("Stopping prometheus scrape server listening on: {}", this.baseUri);
            final GrizzlyFuture<HttpServer> shutdown = httpServer.shutdown(5, TimeUnit.SECONDS);
            log.info("Awaiting termination of prometheus scrape server");
            shutdown.get();
            log.info("prometheus scrape server terminated");
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }

    @Path("/metrics")
    public static class PrometheusResource {
        private final PrometheusMeterRegistry promRegistry;

        public PrometheusResource(PrometheusMeterRegistry promRegistry) {
            this.promRegistry = promRegistry;
        }

        @GET
        public Response scrape() {
            return Response.ok(this.promRegistry.scrape(), "text/plain").build();
        }
    }
}
