/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega;

import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.Cleanup;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient.getClient;


@RunWith(SystemTestRunner.class)
public class PravegaTest {


    private final static String STREAM_NAME = "testStreamSample";
    private final static String STREAM_SCOPE = "testScopeSample";
    private final static String READER_GROUP = "ExampleReaderGroup";
    private final ScalingPolicy scalingPolicy = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2L, 2, 2);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(STREAM_SCOPE).streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();


    /*
        This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void setup() throws InterruptedException, MarathonException, URISyntaxException {
        AuthEnabledMetronomeClient.deleteAllJobs(getClient());
        //1. check if zk is running, if not start it
        Service zookeeperService = new ZookeeperService("zookeeper");
        if (!zookeeperService.isRunning()) {
            zookeeperService.start(true);
        }

        List<URI> zkUris = zookeeperService.getServiceDetails();
        System.out.println("zookeeper service details:" + zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        //2, check if bk is running, otherwise start, get the zk ip
        Service bookkeeperService = new BookkeeperService("bookkeeper", zkUri);
        if (!bookkeeperService.isRunning()) {
            bookkeeperService.start(true);
        }

        List<URI> bkUris = bookkeeperService.getServiceDetails();
        System.out.println("bookkeeper service details:" + bkUris);

        //4.start host
        Service segmentService = new PravegaSegmentStoreService("host", zkUri);

        if (!segmentService.isRunning()) {
            segmentService.start(true);
        }

        List<URI> segUris = segmentService.getServiceDetails();
        System.out.println("pravega host service details:" + segUris);
        URI segUri = segUris.get(0);

        //3. start controller
        Service controllerService = new PravegaControllerService("controller", zkUri, segUri);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }

        List<URI> conUris = controllerService.getServiceDetails();
        System.out.println("Pravega Controller service details:" + conUris);
        URI controllerUri = conUris.get(0);

    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /*
          * Invoke the producer test, ensure we are able to produce 100 messages to the stream.z
          * The test fails incase of exceptions while writing to the stream.
          */

    @Test
    public void producerTest() throws InterruptedException, URISyntaxException {

        Service controllerService = new PravegaControllerService("controller", null, null);
        List<URI> ctlURIs = controllerService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        System.out.println("Invoking producer test.");

        System.out.println("Controller URI: " + controllerUri);

        ControllerImpl controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());

        //create a stream.
        try {
            CompletableFuture<CreateScopeStatus> scopeStatus = controller.createScope(STREAM_SCOPE);
            System.out.println("create scope status" + scopeStatus.get());
            CompletableFuture<CreateStreamStatus> status = controller.createStream(config);
            System.out.println("create stream status" + status.get());
        } catch (ExecutionException e) {
            System.out.println("error in doing a get on create stream status," + e);
            e.printStackTrace();
        }

        Thread.sleep(30000);

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, controllerUri);

        @Cleanup
        EventStreamWriter<Serializable> producer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        for (int i = 0; i < 5; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            producer.writeEvent("", event);
            producer.flush();
            Thread.sleep(2000);
        }
        Thread.sleep(10000);
    }

    /*
     * Invoke consumer test, ensure we are able to read 100 messages from the stream.
     * The test fails incase of exceptions/ timeout.
     */
    @Test
    public void consumerTest() throws URISyntaxException {

        Service controllerService = new PravegaControllerService("controller", null, null);
        List<URI> ctlURIs = controllerService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);
        String string = "http://" + controllerUri.getHost() + ":9090";
        controllerUri = new URI(string);

        System.out.println("Invoking consumer test.");
        System.out.println("Controller URI: " + controllerUri);

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, controllerUri);

        ReaderGroupManager.withScope(STREAM_SCOPE, controllerUri)
                .createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().startingTime(0).build(),
                        Collections.singletonList(STREAM_NAME));

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        for (int i = 0; i < 5; i++) {
            String event = null;
            try {
                event = reader.readNextEvent(6000).getEvent();
            } catch (ReinitializationRequiredException e) {
                e.printStackTrace();
            }
            System.err.println("Read event: " + event);
        }
        reader.close();
        System.exit(0);

    }

}




