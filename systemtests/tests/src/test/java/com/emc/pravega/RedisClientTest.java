/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.MetronomeClientNautilus;
import com.emc.pravega.framework.services.RedisService;
import com.emc.pravega.framework.services.Service;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.net.URI;
import java.util.List;

import static com.emc.pravega.framework.metronome.MetronomeClientNautilus.getClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SystemTestRunner.class)
public class RedisClientTest {

    private static Service redis = new RedisService("redisservice");

    /*
        This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void setup() {
        MetronomeClientNautilus.deleteAllJobs(getClient());
        if (!redis.isRunning()) {
            redis.start(true);
        }
        List<URI> uris = redis.getServiceDetails();
        System.out.println("Redis service details:" + uris);
        assertTrue(uris.size() == 1);
    }

    @BeforeClass
    public static void beforeClass() {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /*
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */
    @Test
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
