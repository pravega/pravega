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
import io.pravega.client.control.impl.Controller;
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
import org.junit.rules.Timeout;
import org.junit.Rule;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BucketDistributionTest extends AbstractSystemTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(3 * 60);

    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");
    private Service controllerService = null;
    private Service segmentStoreService = null;
    private URI controllerURIDirect = null;

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 1);
        ensureSegmentStoreRunning(zkUri, controllerUri);
        log.info("Controller is currently running at {}", controllerUri);
    }

    @Before
    public void getControllerInfo() {
        Service zkService = Utils.createZookeeperService();
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        controllerService = Utils.createPravegaControllerService(zkUris.get(0));
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }

        List<URI> controllerUris = controllerService.getServiceDetails();
        log.info("Controller uris: {} {}", controllerUris.get(0), controllerUris.get(1));
        log.debug("Pravega Controller service  details: {}", controllerUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = controllerUris.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

        controllerURIDirect = URI.create((Utils.TLS_AND_AUTH_ENABLED ? TLS : TCP) + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        segmentStoreService = Utils.createPravegaSegmentStoreService(zkUris.get(0), controllerUris.get(0));
    }

    @After
    public void tearDown() {
        // This test scales the controller instances to 0.
        // Scale down the segment store instances to 0 to ensure the next tests starts with a clean slate.
        segmentStoreService.scaleService(0);
        ExecutorServiceHelpers.shutdown(executorService);
    }

    @Test(timeout = 300000)
    public void controllerToBucketMappingTest() throws Exception {
        log.info("Start execution of controllerToBucketMappingTest");
        log.info("Test tcp:// with 1 controller instances running");

        //Here using delayed future as minDurationForBucketDistribution is set to 2 sec.
        Futures.delayedFuture(Duration.ofSeconds(2), executorService).join();
        @Cleanup
        Controller controller = getController();
        Map<String, List<Integer>> retentionMap = controller.getControllerToBucketMapping(BucketType.RetentionService).join();
        log.info("Controller to bucket mapping for {} is {}.", BucketType.RetentionService, retentionMap);
        List<String> controllerInstances;
        controllerInstances = new ArrayList<>(retentionMap.keySet());
        assertEquals(1, controllerInstances.size());
        assertEquals(1, retentionMap.get(controllerInstances.get(0)).size());

        Map<String, List<Integer>> waterMarkingMap = controller.getControllerToBucketMapping(BucketType.WatermarkingService).join();
        log.info("Controller to bucket mapping for {} is {}.", BucketType.WatermarkingService, waterMarkingMap);
        controllerInstances = new ArrayList<>(retentionMap.keySet());
        assertEquals(1, waterMarkingMap.size());
        assertEquals(100, waterMarkingMap.get(controllerInstances.get(0)).size());

        Futures.getAndHandleExceptions(controllerService.scaleService(2), ExecutionException::new);
        List<URI> controllerUris = controllerService.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = controllerUris.stream().filter(ISGRPC).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create((Utils.TLS_AND_AUTH_ENABLED ? TLS : TCP) + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        log.info("Test tcp:// with only 2 controller instance running");
        Futures.delayedFuture(Duration.ofSeconds(2), executorService).join();
        controller = getController();
        Map<String, List<Integer>> map = controller.getControllerToBucketMapping(BucketType.WatermarkingService).join();
        log.info("Controller to bucket mapping for {} is {}.", BucketType.WatermarkingService, map);
        controllerInstances = new ArrayList<>(map.keySet());
        assertEquals(2, controllerInstances.size());
        assertEquals(50, map.get(controllerInstances.get(0)).size());
        assertEquals(50, map.get(controllerInstances.get(1)).size());

        Futures.getAndHandleExceptions(controllerService.scaleService(0), ExecutionException::new);
        log.info("No controller instance is running.");
        log.info("controllerToBucketMappingTest execution completed");
    }

    private ControllerImpl getController() {
        ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect);
        // Connect with first controller instance.
        return new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), executorService);
    }

}
