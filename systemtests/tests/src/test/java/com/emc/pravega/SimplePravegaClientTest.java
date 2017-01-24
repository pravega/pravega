/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.Cleanup;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

//@MarathonDistributed -- MarathonDistributed tests: All the tests are executed as marathon jobs.
//@MarathonSequential -- Each test is executed (as a marathon job ) one after another.
//@Local -- Each test is executed locally and not as a marathon job. (Default)
//@FreshSetup -- this is used to indicate the Setup needs to be created afresh.
@Ignore
@RunWith(SystemTestRunner.class)
public class SimplePravegaClientTest {
    private final static String STREAM_NAME = "testStream";
    private final static String STREAM_SCOPE = "testScope";

    @Environment
    public static void setup() {
    /*  Service zk = TF.serviceFactory().getZKService();
        zk.setCPU(2.0);
        zk.instanceCount(3);
        zk.start(false);

        Service pravega = TF.serviceFactory().getPravega();
        pravega.setInstanceCount(3);
        pravega.start(false);

        Service controller = TF.serviceFactory().getController();
        controller.setInstanceCount(3);
        controller.start(false);
    */
    }

    @BeforeClass
    public static void BeforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("Invoking create Stream test.");
        String controllerHost = System.getProperty("controller", "a1.dcos");
        ControllerImpl controller = new ControllerImpl(controllerHost, 9090);
        CreateStreamStatus status = controller.createStream(new
                StreamConfigurationImpl(STREAM_SCOPE, STREAM_NAME, new ScalingPolicy(ScalingPolicy
                .Type.FIXED_NUM_SEGMENTS, 0, 0, 1))).get(60000, TimeUnit.MICROSECONDS);
        assertEquals(CreateStreamStatus.SUCCESS, status);
    }

    /*
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */
    @Test
    //@InstanceCount(3)
    public void producerTest() throws URISyntaxException, InterruptedException {
        System.out.println("Invoking producer test.");

        String controllerHost = System.getProperty("controller", "a1.dcos");

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, new URI("tcp://" + controllerHost +
                ":9090"));

        @Cleanup
        EventStreamWriter<Serializable> producer = clientFactory.createEventWriter(STREAM_NAME,
                new JavaSerializer<>(),
                new EventWriterConfig(null));

        for (int i = 0; i < 100; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            producer.writeEvent("", event);
            producer.flush();
            Thread.sleep(500);
        }
    }

    /*
     * Invoke consumer test, ensure we are able to read 100 messages from the stream.
     * The test fails incase of exceptions/ timeout.
     */
    @Test
    public void consumerTest() throws URISyntaxException {
        System.out.println("Invoking consumer test.");
        String controllerHost = System.getProperty("controller", "a1.dcos");
        ControllerImpl controller = new ControllerImpl(controllerHost, 9090);

        ClientFactory clientFactory = ClientFactory.withScope(STREAM_SCOPE, new URI("tcp://" + controllerHost +
                ":9090"));

        @Cleanup
        EventStreamReader<String> consumer = clientFactory.createReader(STREAM_NAME, new
                        JavaSerializer<>(), new ReaderConfig(),
                new PositionImpl(Collections.singletonMap(new Segment(STREAM_SCOPE, STREAM_NAME, 0), 0L), Collections
                        .emptyMap()));
        for (int i = 0; i < 300; i++) {
            String event = consumer.readNextEvent(60000).getEvent();
            assertNotNull(event); //null indicates there was a timeout.
            System.err.println("Read event: " + event);
        }
    }


    @Test
    public void createStreamTest() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("Invoking create Stream test.");
        String controllerHost = System.getProperty("controller", "a1.dcos");
        ControllerImpl controller = new ControllerImpl(controllerHost, 9090);
        CreateStreamStatus status = controller.createStream(new
                StreamConfigurationImpl(STREAM_SCOPE, STREAM_NAME, new ScalingPolicy(ScalingPolicy
                .Type.FIXED_NUM_SEGMENTS, 0, 0, 1))).get(60000, TimeUnit.MICROSECONDS);
        assertEquals(CreateStreamStatus.SUCCESS, status);
    }
}
