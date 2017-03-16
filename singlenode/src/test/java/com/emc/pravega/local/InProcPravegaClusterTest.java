/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.local;

import org.junit.Assert;
import org.junit.Ignore;

import static com.emc.pravega.testcommon.AssertExtensions.assertThrows;

public class InProcPravegaClusterTest {
    private InProcPravegaCluster.InProcPravegaClusterBuilder builder;
    private InProcPravegaCluster cluster;

    @Ignore
    public void initialize() throws Exception {
        builder = InProcPravegaCluster.builder()
                .isInprocController(true)
                .controllerCount(1)
                .isInprocHost(true)
                .hostCount(3)
                .isInProcDL(false)
                .zkUrl("localhost:4000")
                .isInProcHDFS(true);
        cluster = builder.build();
        cluster.setControllerPorts(new int[]{9090});
        cluster.setHostPorts(new int[]{ 5000, 5001, 5002});
        cluster.start();
    }

    @Ignore
    public void testGetControllerUri() {
        Assert.assertEquals(cluster.getControllerURI(), "localhost:9090");
    }

    @Ignore
    public void oneHostOneController() throws Exception {
        cluster.stopHost(1);
    }

    @Ignore
    public void twoHostOneController() throws Exception {
        cluster.startLocalHost(1);

    }

    @Ignore
    public void checkInProcDlNotSupported() throws Exception {
        InProcPravegaCluster cluster = InProcPravegaCluster.builder()
                .isInprocController(true)
                .controllerCount(1)
                .isInprocHost(true)
                .hostCount(1)
                .isInProcDL(true)
                .zkUrl("localhost:4000")
                .isInProcHDFS(true)
                .build();

        cluster.setControllerPorts(new int[]{9090});
        cluster.setHostPorts(new int[]{ 5000});

        assertThrows( "In proc DL not supported",
                () -> {
                        cluster.start();
                },
                ex -> ex instanceof IllegalStateException);

    }

}
