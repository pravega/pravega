/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.testcommon.AssertExtensions;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for InMemoryClusterImpl.
 */
public class InMemoryClusterImplTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testCluster() {
        Cluster cluster = new InMemoryClusterImpl(new Host("localhost", 9090));
        Set<Host> clusterMembers = null;
        try {
            clusterMembers = cluster.getClusterMembers();
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertEquals(1, clusterMembers.size());
        Assert.assertTrue(clusterMembers.contains(new Host("localhost", 9090)));

        AssertExtensions.assertThrows(NotImplementedException.class, () -> {
            cluster.registerHost(null);
            Assert.fail();
        });

        AssertExtensions.assertThrows(NotImplementedException.class, () -> {
            cluster.deregisterHost(null);
            Assert.fail();
        });

        AssertExtensions.assertThrows(NotImplementedException.class, () -> {
            cluster.addListener(null);
            Assert.fail();
        });

        AssertExtensions.assertThrows(NotImplementedException.class, () -> {
            cluster.addListener(null, null);
            Assert.fail();
        });
    }
}
