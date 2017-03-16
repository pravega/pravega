/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.local;

import com.twitter.distributedlog.LocalDLMEmulator;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private static final int NUM_BOOKIES = 3;
    private final InProcPravegaCluster inProcPravegaCluster;

    @Builder
    private LocalPravegaEmulator(int zkPort, int controllerPort, int hostPort) {
        inProcPravegaCluster = InProcPravegaCluster
                .builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + zkPort)
                .zkPort(zkPort)
                .isInMemStorage(true)
                .isInprocController(true)
                .controllerCount(1)
                .isInprocHost(true)
                .hostCount(1)
                .containerCount("2")
                .build();
        inProcPravegaCluster.setControllerPorts(new int[] {controllerPort});
        inProcPravegaCluster.setHostPorts(new int[] {hostPort});
    }

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                log.warn("Usage: LocalPravegaEmulator <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            int zkPort = Integer.parseInt(args[0]);
            final int controllerPort = Integer.parseInt(args[1]);
            final int hostPort = Integer.parseInt(args[2]);

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder().controllerPort(
                    controllerPort).hostPort(hostPort).zkPort(zkPort).build();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                        log.warn("Exception running local Pravega emulator: " + e.getMessage());
                    }
                }
            });

            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Stop controller and host.
     */
    @Override
    public void close() {
       inProcPravegaCluster.close();
    }

    /**
     * Start controller and host.
     */
    private void start() throws Exception {
        inProcPravegaCluster.start();
    }



}
