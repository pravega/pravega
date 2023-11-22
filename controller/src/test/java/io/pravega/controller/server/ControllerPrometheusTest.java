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
package io.pravega.controller.server;

import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * ControllerServiceStarter tests.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class ControllerPrometheusTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    protected StoreClientConfig storeClientConfig;
    protected StoreClient storeClient;
    protected final int restPort;
    protected final int grpcPort;

    public ControllerPrometheusTest() {
        this.grpcPort = TestUtils.getAvailableListenPort();
        this.restPort = TestUtils.getAvailableListenPort();
        this.storeClientConfig = StoreClientConfigImpl.withInMemoryClient();
        this.storeClient = StoreClientFactory.createStoreClient(storeClientConfig);
    }

    @Test
    public void testPrometheusMetrics() throws Exception {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_PROMETHEUS, true)
                .with(MetricsConfig.METRICS_PREFIX, "promtestcontroller")
                .build());

        @Cleanup
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();

        @Cleanup
        ControllerServiceStarter starter = new ControllerServiceStarter(createControllerServiceConfig(), storeClient, null);
        starter.startAsync();
        starter.awaitRunning();

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

        starter.stopAsync();
        starter.awaitTerminated();
    }

    protected ControllerServiceConfig createControllerServiceConfig() {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST, Config.SERVICE_PORT,
                        Config.HOST_STORE_CONTAINER_COUNT))
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .build();

        return ControllerServiceConfigImpl.builder()
                .threadPoolSize(15)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.empty())
                .grpcServerConfig(Optional.of(GRPCServerConfigImpl.builder()
                        .port(grpcPort)
                        .build()))
                .restServerConfig(Optional.of(RESTServerConfigImpl.builder()
                        .port(restPort)
                        .host("localhost")
                        .build()))
                .minBucketRedistributionIntervalInSeconds(10)
                .build();
    }
}
