/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import static org.junit.Assert.assertTrue;
import io.pravega.ClientFactory;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.Transaction;
import io.pravega.stream.impl.ControllerImpl;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.StreamImpl;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.stream.impl.Controller;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RunWith(SystemTestRunner.class)
public class AutoScaleTest extends AbstractScaleTests {

    private final static String SCOPE = "testAutoScale" + new Random().nextInt(Integer.MAX_VALUE);
    private final static String SCALE_UP_STREAM_NAME = "testScaleUp";
    private final static String SCALE_UP_TXN_STREAM_NAME = "testTxnScaleUp";
    private final static String SCALE_DOWN_STREAM_NAME = "testScaleDown";

    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 1);
    private static final StreamConfiguration CONFIG_UP = StreamConfiguration.builder().scope(SCOPE)
            .streamName(SCALE_UP_STREAM_NAME).scalingPolicy(SCALING_POLICY).build();

    private static final StreamConfiguration CONFIG_TXN = StreamConfiguration.builder().scope(SCOPE)
            .streamName(SCALE_UP_TXN_STREAM_NAME).scalingPolicy(SCALING_POLICY).build();

    private static final StreamConfiguration CONFIG_DOWN = StreamConfiguration.builder().scope(SCOPE)
            .streamName(SCALE_DOWN_STREAM_NAME).scalingPolicy(SCALING_POLICY).build();
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

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

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega host service details: {}", segUris);
        URI segUri = segUris.get(0);
    }

    /**
     * Invoke the createStream method, ensure we are able to create stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void createStream() throws InterruptedException, URISyntaxException, ExecutionException {

        //create a scope
        URI controllerUri = getControllerURI();
        Controller controller = getController(controllerUri);

        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("create scope status {}", createScopeStatus);
        
        //create a stream
        Boolean createStreamStatus = controller.createStream(CONFIG_UP).get();
        log.debug("create stream status for scale up stream {}", createStreamStatus);

        createStreamStatus = controller.createStream(CONFIG_DOWN).get();
        log.debug("create stream status for scaledown stream {}", createStreamStatus);

        log.debug("scale down stream starting segments:" + controller.getCurrentSegments(SCOPE, SCALE_DOWN_STREAM_NAME).get().getSegments().size());

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        Boolean status = controller.scaleStream(new StreamImpl(SCOPE, SCALE_DOWN_STREAM_NAME),
                                                Collections.singletonList(0),
                                                keyRanges)
                                   .get();
        assertTrue(status);

        createStreamStatus = controller.createStream(CONFIG_TXN).get();
        log.debug("create stream status for txn stream {}", createStreamStatus);
    }

    @Test
    public void scaleTests() throws URISyntaxException, InterruptedException {
        URI controllerUri = getControllerURI();
        CompletableFuture<Void> scaleup = scaleUpTest(controllerUri);
        CompletableFuture<Void> scaleDown = scaleDownTest(controllerUri);
        CompletableFuture<Void> scalewithTxn = scaleUpTxnTest(controllerUri);
        FutureHelpers.getAndHandleExceptions(CompletableFuture.allOf(scaleup, scaleDown, scalewithTxn)
                .whenComplete((r, e) -> {
                    recordResult(scaleup, "ScaleUp");
                    recordResult(scaleDown, "ScaleDown");
                    recordResult(scalewithTxn, "ScaleWithTxn");

                }), RuntimeException::new);
    }

    /**
     * Invoke the simple scale up Test, produce traffic from multiple writers in parallel.
     * The test will periodically check if a scale event has occured by talking to controller via
     * controller client.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleUpTest(final URI controllerUri) throws InterruptedException,
            URISyntaxException {

        ClientFactory clientFactory = getClientFactory(SCOPE);

        ControllerImpl controller = getController(controllerUri);

        final AtomicBoolean exit = new AtomicBoolean(false);

        startNewWriter(clientFactory, exit);
        startNewWriter(clientFactory, exit);
        startNewWriter(clientFactory, exit);
        startNewWriter(clientFactory, exit);
        startNewWriter(clientFactory, exit);
        startNewWriter(clientFactory, exit);

        // overall wait for test to complete in 260 seconds (4.2 minutes) or scale up, whichever happens first.
        return Retry.withExpBackoff(10, 10, 30, Duration.ofSeconds(10).toMillis())
                .retryingOn(ScaleOperationNotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, SCALE_UP_STREAM_NAME)
                        .thenAccept(x -> {
                            log.debug("size ==" + x.getSegments().size());
                            if (x.getSegments().size() == 1) {
                                throw new ScaleOperationNotDoneException();
                            } else {
                                log.info("scale up done successfully");

                                exit.set(true);
                            }
                        }), EXECUTOR_SERVICE);
    }

    /**
     * Invoke the simple scale down Test, produce no into a stream.
     * The test will periodically check if a scale event has occured by talking to controller via
     * controller client.
     *
     * @param controllerUri Controller URI
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleDownTest(final URI controllerUri) throws InterruptedException, URISyntaxException {

        final ControllerImpl controller = getController(controllerUri);

        final AtomicBoolean exit = new AtomicBoolean(false);

        // overall wait for test to complete in 260 seconds (4.2 minutes) or scale down, whichever happens first.
        return Retry.withExpBackoff(10, 10, 30, Duration.ofSeconds(10).toMillis())
                .retryingOn(ScaleOperationNotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, SCALE_DOWN_STREAM_NAME)
                        .thenAccept(x -> {
                            if (x.getSegments().size() == 2) {
                                throw new ScaleOperationNotDoneException();
                            } else {
                                log.info("scale down done successfully");

                                exit.set(true);
                            }
                        }), EXECUTOR_SERVICE);
    }

    /**
     * Invoke the scale up Test with transactional writes. Produce traffic from multiple writers in parallel.
     * Each writer writes using transactions.
     * Transactions are committed quickly to give
     * The test will periodically check if a scale event has occured by talking to controller via
     * controller client.
     *
     * @param controllerUri Controller URI
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleUpTxnTest(final URI controllerUri) throws InterruptedException,
            URISyntaxException {

        ControllerImpl controller = getController(controllerUri);

        final AtomicBoolean exit = new AtomicBoolean(false);

        ClientFactory clientFactory = getClientFactory(SCOPE);
        startNewTxnWriter(clientFactory, exit);

        // overall wait for test to complete in 260 seconds (4.2 minutes) or scale up, whichever happens first.
        return Retry.withExpBackoff(10, 10, 30, Duration.ofSeconds(10).toMillis())
                .retryingOn(ScaleOperationNotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, SCALE_UP_TXN_STREAM_NAME)
                        .thenAccept(x -> {
                            if (x.getSegments().size() == 1) {
                                throw new ScaleOperationNotDoneException();
                            } else {
                                log.info("txn test scale up done successfully");
                                exit.set(true);
                            }
                        }), EXECUTOR_SERVICE);
    }

    private void startNewWriter(ClientFactory clientFactory, AtomicBoolean exit) {
        CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(SCALE_UP_STREAM_NAME,
                    new JavaSerializer<>(),
                    EventWriterConfig.builder().build());

            while (!exit.get()) {
                try {
                    writer.writeEvent("0", "test").get();
                } catch (Throwable e) {
                    log.warn("test exception writing events: {}", e);
                    break;
                }
            }
        });
    }

    private void startNewTxnWriter(ClientFactory clientFactory, AtomicBoolean exit) {
        CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<String> writer = clientFactory.createEventWriter(SCALE_UP_TXN_STREAM_NAME,
                    new JavaSerializer<>(),
                    EventWriterConfig.builder().build());

            while (!exit.get()) {
                try {
                    Transaction<String> transaction = writer.beginTxn(5000, 3600000, 29000);

                    for (int i = 0; i < 100; i++) {
                        transaction.writeEvent("0", "txntest");
                    }

                    transaction.commit();
                } catch (Throwable e) {
                    if (!(e instanceof RuntimeException && e.getCause() != null &&
                            e.getCause() instanceof io.grpc.StatusRuntimeException &&
                            ((io.grpc.StatusRuntimeException) e.getCause()).getStatus().getCode().equals(Status.Code.INTERNAL) &&
                            Objects.equals(((StatusRuntimeException) e.getCause()).getStatus().getDescription(),
                                    "io.pravega.controller.task.Stream.StreamTransactionMetadataTasks not yet ready"))) {
                        log.warn("test exception writing events in a transaction : {}", e);
                        break;
                    }
                }
            }
        });
    }
}
