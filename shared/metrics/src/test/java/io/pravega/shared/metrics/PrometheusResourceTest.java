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

import com.google.common.collect.ImmutableSet;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class PrometheusResourceTest {

    @Test
    public void testStatsProviderNoPrometheus() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, true)
                .build();
        @Cleanup
        CompositeMeterRegistry localRegistry = new CompositeMeterRegistry();

        @Cleanup
        StatsProvider statsProvider = new StatsProviderImpl(appConfig, localRegistry);
        statsProvider.start();

        for (MeterRegistry registry : localRegistry.getRegistries()) {
            assertFalse(registry instanceof PrometheusMeterRegistry);
        }

        assertFalse(statsProvider.prometheusResource().isPresent());

        statsProvider.close();
    }

    @Test
    public void testScrape() throws Exception {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_PROMETHEUS, true)
                .build();
        @Cleanup
        CompositeMeterRegistry localRegistry = new CompositeMeterRegistry();

        @Cleanup
        StatsProvider statsProvider = new StatsProviderImpl(appConfig, localRegistry);
        statsProvider.start();

        for (MeterRegistry registry : localRegistry.getRegistries()) {
            assertTrue(registry instanceof PrometheusMeterRegistry);
        }
        assertTrue(statsProvider.prometheusResource().isPresent());

        ResourceConfig rc = ResourceConfig.forApplication(new PrometheusApplication(statsProvider));
        URI baseUri = UriBuilder.fromUri("http://localhost/")
                .port(TestUtils.getAvailableListenPort())
                .build();

        @Cleanup("shutdown")
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, rc);

        Counter c = statsProvider.createStatsLogger("promtest").createCounter("promtestcounter");
        c.add(1);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + "prometheus"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.body().lines().anyMatch(x -> Pattern.matches("promtestcounter.*1\\.0", x)));

        server.shutdown();
    }

    public static class PrometheusApplication extends Application {
        private StatsProvider statsProvider;

        PrometheusApplication(StatsProvider sp) {
            super();
            this.statsProvider = sp;
        }

        @Override
        public Set<Object> getSingletons() {
            return ImmutableSet.of(this.statsProvider.prometheusResource().get());
        }
    }
}
