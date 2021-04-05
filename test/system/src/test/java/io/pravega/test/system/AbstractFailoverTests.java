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

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertTrue;

/**
 * This class provides useful methods for classes that aim at implementing system tests in which service instance
 * failures are involved during read and write workloads.
 */
@Slf4j
abstract class AbstractFailoverTests extends AbstractReadWriteTest {

    static final String AUTO_SCALE_STREAM = "testReadWriteAndAutoScaleStream";
    static final String SCALE_STREAM = "testReadWriteAndScaleStream";
    //Duration for which the system test waits for writes/reads to happen post failover.
    //10s (SessionTimeout) + 10s (RebalanceContainers) + 20s (For Container recovery + start) + NetworkDelays
    static final int WAIT_AFTER_FAILOVER_MILLIS = 40 * 1000;

    Service controllerInstance;
    Service segmentStoreInstance;
    URI controllerURIDirect = null;
    Controller controller;
    ScheduledExecutorService controllerExecutorService;

    void performFailoverTest() throws ExecutionException {
        log.info("Test with 3 controller, segment store instances running and without a failover scenario");
        long currentWriteCount1 = testState.getEventWrittenCount();
        long currentReadCount1 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} without any failover", currentReadCount1, currentWriteCount1);

        //check reads and writes after sleeps
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

        long currentWriteCount2 = testState.getEventWrittenCount();
        long currentReadCount2 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} without any failover after sleep before scaling", currentReadCount2, currentWriteCount2);
        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down segment store instances to 2
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down Segment Store instances from 3 to 2");

        currentWriteCount1 = testState.getEventWrittenCount();
        currentReadCount1 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} after Segment Store failover after sleep", currentReadCount1, currentWriteCount1);
        //ensure writes are happening
        assertTrue(currentWriteCount1 > currentWriteCount2);
        //ensure reads are happening
        assertTrue(currentReadCount1 > currentReadCount2);

        //Scale down controller instances to 2
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down controller instances from 3 to 2");

        currentWriteCount2 = testState.getEventWrittenCount();
        currentReadCount2 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} after controller failover after sleep", currentReadCount2, currentWriteCount2);
        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down segment  store, controller to 1 instance each.
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down to 1 controller, 1 Segment Store instance");

        currentWriteCount1 = testState.getEventWrittenCount();
        currentReadCount1 = testState.getEventReadCount();
        log.info("Stop write flag status: {}, stop read flag status: {} ", testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with Segment Store  and controller failover after sleep", currentReadCount1, currentWriteCount1);
    }

    void performFailoverForTestsInvolvingTxns() throws ExecutionException {
        log.info("Test with 3 controller, segment store instances running and without a failover scenario");
        log.info("Read count: {}, write count: {} without any failover",
                testState.getEventReadCount(), testState.getEventWrittenCount());

        //check reads and writes after sleeps
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Read count: {}, write count: {} without any failover after sleep before scaling",
                testState.getEventReadCount(), testState.getEventWrittenCount());

        //Scale down segment store instances to 2
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down Segment Store instances from 3 to 2");
        log.info("Read count: {}, write count: {} after Segment Store  failover after sleep",
                testState.getEventReadCount(), testState.getEventWrittenCount());

        //Scale down controller instances to 2
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down controller instances from 3 to 2");
        log.info("Read count: {}, write count: {} after controller failover after sleep",
                testState.getEventReadCount(), testState.getEventWrittenCount());

        //Scale down segment store, controller to 1 instance each.
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down  to 1 controller, 1 Segment Store  instance");
        log.info("Stop write flag status: {}, stop read flag status: {} ",
                testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with Segment Store  and controller failover after sleep",
                testState.getEventReadCount(), testState.getEventWrittenCount());
    }

    void createScopeAndStream(String scope, String stream, StreamConfiguration config, StreamManager streamManager) {
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Creating scope with scope name {}", scope);
        log.debug("Create scope status {}", createScopeStatus);
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.debug("Create stream status {}", createStreamStatus);
    }

    void cleanUp(String scope, String stream, ReaderGroupManager readerGroupManager, String readerGroupName) throws InterruptedException, ExecutionException {
        CompletableFuture<Boolean> sealStreamStatus = Retry.indefinitelyWithExpBackoff("Failed to seal stream. retrying ...")
                                                           .runAsync(() -> controller.sealStream(scope, stream), executorService);
        log.info("Sealing stream {}", stream);
        assertTrue(sealStreamStatus.get());
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, stream);
        log.info("Deleting stream {}", stream);
        assertTrue(deleteStreamStatus.get());
        log.info("Deleting readergroup {}", readerGroupName);
        readerGroupManager.deleteReaderGroup(readerGroupName);
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
    }

    void waitForScaling(String scope, String stream, StreamConfiguration initialConfig) {
        int initialMaxSegmentNumber = initialConfig.getScalingPolicy().getMinNumSegments() - 1;
        boolean scaled = false;
        for (int waitCounter = 0; waitCounter < SCALE_WAIT_ITERATIONS; waitCounter++) {
            StreamSegments streamSegments = controller.getCurrentSegments(scope, stream).join();
            if (streamSegments.getSegments().stream().mapToLong(Segment::getSegmentId).max().orElse(-1) > initialMaxSegmentNumber) {
                scaled = true;
                break;
            }
            //Scaling operation did not happen, wait
            Exceptions.handleInterrupted(() -> Thread.sleep(10000));
        }

        assertTrue("Scaling did not happen within desired time", scaled);
    }
}
