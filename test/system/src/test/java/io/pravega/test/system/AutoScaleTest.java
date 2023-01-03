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

import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class AutoScaleTest extends AbstractScaleTests {

    private static final String SCALE_UP_STREAM_NAME = "testScaleUp";
    private static final String SCALE_UP_TXN_STREAM_NAME = "testTxnScaleUp";
    private static final String SCALE_DOWN_STREAM_NAME = "testScaleDown";

    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 1);
    private static final StreamConfiguration CONFIG_UP = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).build();

    private static final StreamConfiguration CONFIG_TXN = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).build();

    private static final StreamConfiguration CONFIG_DOWN = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).build();

    //The execution time for @Before + @After + @Test methods should be less than 10 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10 * 60);

    private final ScheduledExecutorService scaleExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "autoscaletest");

    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the createStream method, ensure we are able to create stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void setup() throws InterruptedException, ExecutionException {

        //create a scope
        Controller controller = getController();
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "AutoScaleTest-main");
        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(SCOPE, SCALE_UP_STREAM_NAME, CONFIG_UP).get();
        log.debug("create stream status for scale up stream {}", createStreamStatus);

        createStreamStatus = controller.createStream(SCOPE, SCALE_DOWN_STREAM_NAME, CONFIG_DOWN).get();
        log.debug("create stream status for scaledown stream {}", createStreamStatus);

        log.debug("scale down stream starting segments:" + controller.getCurrentSegments(SCOPE, SCALE_DOWN_STREAM_NAME).get().getSegments().size());

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        Boolean status = controller.scaleStream(new StreamImpl(SCOPE, SCALE_DOWN_STREAM_NAME),
                Collections.singletonList(0L),
                keyRanges,
                executorService).getFuture().get();
        assertTrue(status);

        createStreamStatus = controller.createStream(SCOPE, SCALE_UP_TXN_STREAM_NAME, CONFIG_TXN).get();
        log.debug("create stream status for txn stream {}", createStreamStatus);
    }

    @After
    public void tearDown() {
        getClientFactory().close();
        getConnectionFactory().close();
        getController().close();
        ExecutorServiceHelpers.shutdown(executorService, scaleExecutorService);
    }

    @Test
    public void scaleTests() {
        testState = new TestState(false);
        CompletableFuture<Void> scaleup = scaleUpTest();
        CompletableFuture<Void> scaleDown = scaleDownTest();
        CompletableFuture<Void> scalewithTxn = scaleUpTxnTest();
        Futures.getAndHandleExceptions(CompletableFuture.allOf(scaleup, scaleDown, scalewithTxn)
                                                        .whenComplete((r, e) -> {
                    recordResult(scaleup, "ScaleUp");
                    recordResult(scaleDown, "ScaleDown");
                    recordResult(scalewithTxn, "ScaleWithTxn");

                }), RuntimeException::new);
    }

    /**
     * Invoke the simple scale up Test, produce traffic from multiple writers in parallel.
     * The test will periodically check if a scale event has occurred by talking to controller via
     * controller client.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleUpTest() {
        ClientFactoryImpl clientFactory = getClientFactory();
        ControllerImpl controller = getController();
        final AtomicBoolean exit = new AtomicBoolean(false);
        createWriters(clientFactory, 6, SCOPE, SCALE_UP_STREAM_NAME);

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
                        }), scaleExecutorService);
    }

    /**
     * Invoke the simple scale down Test, produce no into a stream.
     * The test will periodically check if a scale event has occurred by talking to controller via
     * controller client.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleDownTest() {
        final ControllerImpl controller = getController();

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
                            }
                        }), scaleExecutorService);
    }

    /**
     * Invoke the scale up Test with transactional writes. Produce traffic from multiple writers in parallel. Each
     * writer writes using transactions. The test will periodically check if a scale event has occurred by talking to
     * controller via controller client.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    private CompletableFuture<Void> scaleUpTxnTest() {
        ControllerImpl controller = getController();
        final AtomicBoolean exit = new AtomicBoolean(false);
        ClientFactoryImpl clientFactory = getClientFactory();
        startWritingIntoTxn(clientFactory.createTransactionalEventWriter("writer", SCALE_UP_TXN_STREAM_NAME, new JavaSerializer<>(),
                EventWriterConfig.builder().build()), exit);

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
                        }).thenRun(() -> controller.listCompletedTransactions(Stream.of(SCOPE, SCALE_UP_TXN_STREAM_NAME))
                                        .thenAccept(txnList -> log.info("No of completed txn {}", txnList.size()))),
                        scaleExecutorService);
    }
}
