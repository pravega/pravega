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
package io.pravega.controller.util;

import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for Config.
 */
public class ConfigTest {

    /**
     * Verify that all keys are loaded properly from the test/resources/application.conf file.
     */
    @Test
    public void testConfigValues() {
        Assert.assertEquals(9090, Config.RPC_SERVER_PORT);
        Assert.assertEquals(20, Config.ASYNC_TASK_POOL_SIZE);
        Assert.assertEquals(9090, Config.RPC_PUBLISHED_SERVER_PORT);
        Assert.assertEquals("localhost", Config.SERVICE_HOST);
        Assert.assertEquals(12345, Config.SERVICE_PORT);
        Assert.assertEquals(4, Config.HOST_STORE_CONTAINER_COUNT);
        Assert.assertEquals(false, Config.HOST_MONITOR_ENABLED);
        Assert.assertEquals("pravega-cluster", Config.CLUSTER_NAME);
        Assert.assertEquals(10, Config.CLUSTER_MIN_REBALANCE_INTERVAL);
        Assert.assertEquals("localhost:2181", Config.ZK_URL);
        Assert.assertEquals(100, Config.ZK_RETRY_SLEEP_MS);
        Assert.assertEquals(5, Config.ZK_MAX_RETRIES);
        Assert.assertEquals(10 * 1000, Config.ZK_SESSION_TIMEOUT_MS);
        Assert.assertEquals("0.0.0.0", Config.REST_SERVER_IP);
        Assert.assertEquals(9091, Config.REST_SERVER_PORT);
        Assert.assertEquals(30000, Config.MAX_LEASE_VALUE);
        Assert.assertEquals("_requeststream", Config.SCALE_STREAM_NAME);
        Assert.assertEquals("scaleGroup", Config.SCALE_READER_GROUP);
    }

    @Test
    public void testGRPCConfig() {
        GRPCServerConfig grpcServerConfig = Config.GRPC_SERVER_CONFIG;
        Assert.assertEquals(9090, grpcServerConfig.getPort());
        Assert.assertEquals(9090, (int) grpcServerConfig.getPublishedRPCPort().orElse(12345));
        Assert.assertFalse(grpcServerConfig.getPublishedRPCHost().isPresent());
    }

    @Test
    public void testMetricsConfig() {
        val mc = Config.METRICS_CONFIG;
        Assert.assertEquals("no-host", mc.getStatsDHost());
    }
}
