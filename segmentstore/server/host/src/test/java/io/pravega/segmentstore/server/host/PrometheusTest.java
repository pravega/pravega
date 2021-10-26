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
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Test the functionality of ServiceStarter used to setup segment store.
 */
@RunWith(SerializedClassRunner.class)
public class PrometheusTest {

    private ServiceStarter serviceStarter;
    private TestingServer zkTestServer;
    private int restPort;

    @Before
    public void setup() throws Exception {
        this.restPort = TestUtils.getAvailableListenPort();
        zkTestServer = new TestingServerStarter().start();
        String zkUrl = zkTestServer.getConnectString();
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(MetricsConfig.builder()
                        .with(MetricsConfig.ENABLE_STATISTICS, true)
                        .with(MetricsConfig.ENABLE_PROMETHEUS, true)
                        .with(MetricsConfig.METRICS_PREFIX, "promtestsegmentstore")
                )
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.ZK_URL, zkUrl)
                        .with(ServiceConfig.REST_LISTENING_ENABLE, true)
                        .with(ServiceConfig.REST_LISTENING_PORT, this.restPort)
                );
        serviceStarter = new ServiceStarter(configBuilder.build());
        serviceStarter.start();
    }

    @After
    public void stopZookeeper() throws Exception {
        serviceStarter.shutdown();
        zkTestServer.close();
    }

    @Test
    public void testPrometheusMetrics() throws Exception {
        @Cleanup
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();

        Counter c = statsProvider.createStatsLogger("promtest").createCounter("promtestcounter");
        c.add(1);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + this.restPort + "/prometheus"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.body().lines().anyMatch(x -> Pattern.matches("promtestcounter.*1\\.0", x)));
    }
}
