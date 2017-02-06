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

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.RedisService;
import com.emc.pravega.framework.Service;
import com.emc.pravega.framework.SystemTestRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//@FreshSetup -- this is used to indicate the Setup needs to be created afresh.
@RunWith(SystemTestRunner.class)
public class RedisClientTest {
    private final static String STREAM_NAME = "testStream";
    private final static String STREAM_SCOPE = "testScope";

    private static URI resdisHostURI;

    @Environment
    public static void setup() {
        Service redis = new RedisService("redisservice");
        //TODO: set attributes
        if (!redis.isRunning()) {
            redis.start();
        }
        List<URI> uris = redis.getServiceDetails();
        assertTrue(uris.size() == 1);
        resdisHostURI = uris.get(0);

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

    }

    /*
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */
    @Test
    //@InstanceCount(3)
    public void redisPingTest() {
        Jedis redisClient = new Jedis(resdisHostURI.getHost(), resdisHostURI.getPort());
        assertEquals("PONG", redisClient.ping());
    }
}
