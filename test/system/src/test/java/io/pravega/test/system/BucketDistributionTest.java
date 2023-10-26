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
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.BucketType;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BucketDistributionTest extends AbstractSystemTest {

    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
    private Service controllerService = null;
    private Service segmentStoreService = null;
    private final AtomicReference<URI> controllerURIDirect = new AtomicReference<>();
    private final AtomicReference<URI> controllerURIDiscover = new AtomicReference<>();

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUris = startZookeeperInstance();
        startBookkeeperInstances(zkUris);
        URI controllerUri = ensureControllerRunning(zkUris);
        log.info("Controller is currently running at {}", controllerUri);
        Service controllerService = Utils.createPravegaControllerService(zkUris);

        // With Kvs we need segment stores to be running.
        ensureSegmentStoreRunning(zkUris, controllerUri);

        // scale to two controller instances.
        Futures.getAndHandleExceptions(controllerService.scaleService(2), ExecutionException::new);

        List<URI> conUris = controllerService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);
    }

    @Before
    public void getControllerInfo() {
        Service zkService = Utils.createZookeeperService();
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        controllerService = Utils.createPravegaControllerService(zkUris.get(0));

        List<URI> conUris = controllerService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        assertEquals("2 controller instances should be running", 2, uris.size());

        // use the last two uris
        controllerURIDirect.set(URI.create((Utils.TLS_AND_AUTH_ENABLED ? TLS : TCP) + String.join(",", uris)));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        controllerURIDiscover.set(URI.create("pravega://" + String.join(",", uris)));
        log.info("Controller Service discovery URI: {}", controllerURIDiscover);

        segmentStoreService = Utils.createPravegaSegmentStoreService(zkUris.get(0), controllerService.getServiceDetails().get(0));
    }

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executorService);
        // The test scales down the controller instances to 0.
        // Scale down the segment store instances to 0 to ensure the next tests start with a clean slate.
        segmentStoreService.scaleService(0);
    }

    @Test(timeout = 300000)
    public void controllerToBucketMappingTest() throws Exception {
        log.info("Start execution of controllerToBucketMappingTest");

        log.info("Test tcp:// with 2 controller instances running");

        Map<String, List<Integer>> map = getBucketControllerMapping(BucketType.WatermarkingService).join();
        List<String> controllerInstances = new ArrayList<>(map.keySet());
        assertEquals(2, controllerInstances.size());
        assertEquals(50, map.get(controllerInstances.get(0)).size());
        assertEquals(50, map.get(controllerInstances.get(1)).size());

        Futures.getAndHandleExceptions(controllerService.scaleService(1), ExecutionException::new);
        log.info("Test tcp:// with only 1 controller instance running");

        Map<String, List<Integer>> retentionMap = getBucketControllerMapping(BucketType.RetentionService).join();
        controllerInstances = new ArrayList<>(retentionMap.keySet());
        assertEquals(1, controllerInstances.size());
        assertEquals(3, retentionMap.get(controllerInstances.get(0)).size());

        Map<String, List<Integer>> waterMarkingMap = getBucketControllerMapping(BucketType.WatermarkingService).join();
        controllerInstances = new ArrayList<>(retentionMap.keySet());
        assertEquals(1, waterMarkingMap.size());
        assertEquals(100, waterMarkingMap.get(controllerInstances.get(0)).size());

        Futures.getAndHandleExceptions(controllerService.scaleService(0), ExecutionException::new);
        log.info("No controller instance is running.");
        log.info("controllerToBucketMappingTest execution completed");
    }

    private CompletableFuture<Map<String, List<Integer>>> getBucketControllerMapping(BucketType bucketType) {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect.get());
        @Cleanup
        final ControllerImpl controllerClient = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .build(), executorService);
        //Here using delayed future as minDurationForBucketDistribution is set to 2 sec.
        return Futures.delayedFuture(() -> controllerClient.getControllerToBucketMapping(bucketType),
                2000L, executorService);
    }

}
