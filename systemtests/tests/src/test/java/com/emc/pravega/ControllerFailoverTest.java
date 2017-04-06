package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import com.emc.pravega.stream.impl.TxnSegments;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Controller fail over system test.
 */
@Slf4j
@RunWith(SystemTestRunner.class)
public class ControllerFailoverTest {
    private static Service controllerServiceInstance1, controllerServiceInstance2;

    @Environment
    public static void setup() throws InterruptedException, MarathonException, URISyntaxException {

        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //3. start 2 controller instances
        controllerServiceInstance1 = new PravegaControllerService("controller1", zkUri);
        if (!controllerServiceInstance1.isRunning()) {
            controllerServiceInstance1.start(true);
        }

        List<URI> conUris1 = controllerServiceInstance1.getServiceDetails();
        log.debug("Pravega Controller service instance 1 details: {}", conUris1);

        controllerServiceInstance2 = new PravegaControllerService("controller2", zkUri);
        if (!controllerServiceInstance2.isRunning()) {
            controllerServiceInstance2.start(true);
        }

        List<URI> conUris2 = controllerServiceInstance2.getServiceDetails();
        log.debug("Pravega Controller service instance 1 details: {}", conUris2);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris1.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
    }

    @Test
    public void scaleTests() throws URISyntaxException, InterruptedException {
        String scope = "testFailoverScope";
        String stream = "testFailoverStream";
        int initialSegments = 2;
        List<Integer> segmentsToSeal = Collections.singletonList(0);
        Map<Double, Double> newRangesToCreate = new HashMap<>();
        newRangesToCreate.put(0.0, 0.25);
        newRangesToCreate.put(0.25, 0.5);
        long lease = 20000;
        long maxExecutionTime = 60000;
        long scaleGracePeriod = 30000;

        // Connect with first controller instance.
        URI controllerUri = controllerServiceInstance1.getServiceDetails().get(0);
        Controller controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());

        // Create scope, stream, and a transaction with high timeout value.
        controller.createScope(scope).join();
        createStream(controller, scope, stream, ScalingPolicy.fixed(initialSegments));
        TxnSegments txnSegments = controller.createTransaction(
                new StreamImpl(scope, stream), lease, maxExecutionTime, scaleGracePeriod).join();

        // Initiate scale operation. It will block until ongoing transaction is complete.
        CompletableFuture<Boolean> scaleFuture = controller.scaleStream(
                new StreamImpl(scope, stream), segmentsToSeal, newRangesToCreate);

        // Ensure that scale is not yet done.
        Assert.assertTrue(!scaleFuture.isDone());
        // Now stop the controller instance executing scale operation.
        controllerServiceInstance1.stop();

        // Connect to another controller instance.
        controllerUri = controllerServiceInstance2.getServiceDetails().get(0);
        controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());

        // Abort the ongoing transaction.
        controller.abortTransaction(new StreamImpl(scope, stream), txnSegments.getTxnId()).join();

        // Scale operation should now complete on the second controller instance.
        // Sleep for a minute for it to complete
        Thread.sleep(60000);

        // Ensure that the stream has 3 segments now.
        StreamSegments streamSegments = controller.getCurrentSegments(scope, stream).join();
        Assert.assertEquals(initialSegments - segmentsToSeal.size() + newRangesToCreate.size(),
                streamSegments.getSegments().size());
    }

    private void createStream(Controller controller, String scope, String stream, ScalingPolicy scalingPolicy) {
        StreamConfiguration config = StreamConfiguration.builder()
                .scope(scope)
                .streamName(stream)
                .scalingPolicy(scalingPolicy)
                .build();
        controller.createStream(config).join();
    }
}
