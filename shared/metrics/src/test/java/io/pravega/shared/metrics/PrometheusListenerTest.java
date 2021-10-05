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

import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class PrometheusListenerTest {

    @Test
    public void testListener() throws Exception {
        int port = TestUtils.getAvailableListenPort();
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_PROMETHEUS_LISTENER, true)
                .with(MetricsConfig.PROMETHEUS_ADDRESS, "localhost")
                .with(MetricsConfig.PROMETHEUS_PORT, port)
                .build());

        @Cleanup
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();

        Counter c = statsProvider.createStatsLogger("promtest").createCounter("promtestcounter");
        c.add(1);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/metrics"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.body().lines().anyMatch(x -> Pattern.matches("promtestcounter.*1\\.0", x)));
    }
}
