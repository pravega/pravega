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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static java.time.Duration.ofSeconds;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWithAutoScaleTest extends AbstractScaleTests {

    private final static String STREAM_NAME = "testTxnScaleUpWithRead";
    private final static String READER_GROUP_NAME = "testReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    //Initial number of segments is 2.
    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 2);
    private static final StreamConfiguration CONFIG = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).build();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(12 * 60);

    private final ScheduledExecutorService scaleExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the createStream method, ensure we are able to create scope, stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void setup() throws InterruptedException, ExecutionException {
        Controller controller = getController();
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(4, "ReadWithAutoScaleTest-main");

        //create a scope
        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("Create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(SCOPE, STREAM_NAME, CONFIG).get();
        log.debug("Create stream status {}", createStreamStatus);
    }

    @After
    public void tearDown() {
        getClientFactory().close();
        getConnectionFactory().close();
        getController().close();
        ExecutorServiceHelpers.shutdown(executorService, scaleExecutorService);
    }

    @Test
    public void scaleTestsWithReader() {
        URI controllerUri = getControllerURI();
        Controller controller = getController();
        testState = new TestState(true);

        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);

        @Cleanup
        EventStreamClientFactory clientFactory = getClientFactory();

        //1. Start writing events to the Stream.
        List<CompletableFuture<Void>> writers = new ArrayList<>();
        writers.add(startWritingIntoTxn(clientFactory.createTransactionalEventWriter("initWriter", STREAM_NAME, new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(25000).build()), stopWriteFlag));

        //2. Start a reader group with 2 readers (The stream is configured with 2 segments.)

        //2.1 Create a reader group.
        log.info("Creating Reader group : {}", READER_GROUP_NAME);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, Utils.buildClientConfig(controllerUri));
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM_NAME)).build());

        //2.2 Create readers.
        CompletableFuture<Void> reader1 = startReading(clientFactory.createReader("reader1", READER_GROUP_NAME,
                new JavaSerializer<>(), ReaderConfig.builder().build()), stopReadFlag);
        CompletableFuture<Void> reader2 = startReading(clientFactory.createReader("reader2", READER_GROUP_NAME,
                new JavaSerializer<>(), ReaderConfig.builder().build()), stopReadFlag);

        //3 Now increase the number of TxnWriters to trigger scale operation.
        log.info("Increasing the number of writers to 6");
        for (int i = 0; i < 5; i++) {
            writers.add(startWritingIntoTxn(clientFactory.createTransactionalEventWriter("writer-"
                    + i, STREAM_NAME, new JavaSerializer<>(), EventWriterConfig.builder().transactionTimeoutTime(25000).build()), stopWriteFlag));
        }

        //4 Wait until the scale operation is triggered (else time out)
        //    validate the data read by the readers ensuring all the events are read and there are no duplicates.
        CompletableFuture<Void> testResult = Retry.withExpBackoff(10, 10, 40, ofSeconds(10).toMillis())
                .retryingOn(ScaleOperationNotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, STREAM_NAME)
                        .thenAccept(x -> {
                            int currentNumOfSegments = x.getSegments().size();
                            if (currentNumOfSegments == 2) {
                                log.info("The current number of segments is equal to 2, ScaleOperation did not happen");
                                //Scaling operation did not happen, retry operation.
                                throw new ScaleOperationNotDoneException();
                            } else if (currentNumOfSegments > 2) {
                                //scale operation successful.
                                log.info("Current Number of segments is {}", currentNumOfSegments);
                                stopWriteFlag.set(true);
                            } else {
                                Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
                            }
                        }), scaleExecutorService)
                .thenCompose(v -> Futures.allOf(writers))
                .thenRun(this::waitForTxnsToComplete)
                .thenCompose(v -> {
                    stopReadFlag.set(true);
                    log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                            "Count: {}", testState.writtenEvents, testState.readEvents);
                    return CompletableFuture.allOf(reader1, reader2);
                })
                .thenRun(this::validateResults);

        Futures.getAndHandleExceptions(testResult
                .whenComplete((r, e) -> {
                    recordResult(testResult, "ScaleUpWithTxnWithReaderGroup");
                }), RuntimeException::new);
        readerGroupManager.deleteReaderGroup(READER_GROUP_NAME);
    }
}
