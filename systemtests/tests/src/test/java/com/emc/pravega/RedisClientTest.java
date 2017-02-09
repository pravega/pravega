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
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.RedisService;
import com.emc.pravega.framework.services.Service;
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

@RunWith(SystemTestRunner.class)
public class RedisClientTest {
    private final static String STREAM_NAME = "testStream";
    private final static String STREAM_SCOPE = "testScope";

    private static URI resdisHostURI;

    private static Service redis = new RedisService("redisservice");

    private static String beforeClassVariable;

    @Environment
    public static void setup() {
        //TODO: set attributes
        if (!redis.isRunning()) {
            redis.start();
        }
        List<URI> uris = redis.getServiceDetails();
        System.out.println("Redis service details:" + uris);
        assertTrue(uris.size() == 1);

        resdisHostURI = uris.get(0);
    }

    @BeforeClass
    public static void BeforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /*
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */
    @Test
    //@InstanceCount(3)
    public void redisPingTest() {
        System.out.println("Start execution of redisPingTest");
        //Fetch the service details
        URI redisURI = redis.getServiceDetails().get(0);
        System.out.println("Redis Service URI: " + redisURI);

        //Perform test
        Jedis redisClient = new Jedis(redisURI.getHost(), redisURI.getPort());
        assertEquals("PONG", redisClient.ping());

        System.out.println("Test execution completed");
    }

    @Test
    public void redisFailPingTest() {
        //Test to simulate a failed system test.
        System.out.println("Test a failed system test");
        URI redisURI = redis.getServiceDetails().get(0);
        System.out.println("Redis Service URI: " + redisURI);

        Jedis redisClient = new Jedis(redisURI.getHost(), redisURI.getPort());
        assertEquals("INVALID", redisClient.ping()); // this assertion fails

        System.out.println("Test execution completed");
    }

    @Test
    public void redisGetTest() {
        System.out.println("Test a failed system test");
        URI redisURI = redis.getServiceDetails().get(0);
        System.out.println("Redis Service URI: " + redisURI);

        Jedis redisClient = new Jedis(redisURI.getHost(), redisURI.getPort());
        redisClient.set("Key1", "Value1");

        assertEquals("Value1", redisClient.get("Key1"));
    }

}
