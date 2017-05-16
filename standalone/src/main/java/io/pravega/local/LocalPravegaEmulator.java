/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;


import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private final InProcPravegaCluster inProcPravegaCluster;

    @Builder
    private LocalPravegaEmulator(int zkPort, int controllerPort, int segmentStorePort) {
        inProcPravegaCluster = InProcPravegaCluster
                .builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + zkPort)
                .zkPort(zkPort)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .build();
        inProcPravegaCluster.setControllerPorts(new int[] {controllerPort});
        inProcPravegaCluster.setSegmentStorePorts(new int[] {segmentStorePort});
    }

    public static void main(String[] args) {
        try {

            ServiceBuilderConfig config = ServiceBuilderConfig
                    .builder()
                    .include(System.getProperties())
                    .build();
            SingleNodeConfig conf = config.getConfig(SingleNodeConfig::builder);

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder()
                    .controllerPort(conf.getControllerPort())
                    .segmentStorePort(conf.getSegmentStorePort())
                    .zkPort(conf.getZkPort())
                    .build();

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

            log.info("Starting Pravega Emulator with ports: ZK port {}, controllerPort {}, SegmentStorePort {}",
                    conf.getZkPort(), conf.getControllerPort(), conf.getSegmentStorePort());

            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            conf.getControllerPort()));
        } catch (Exception ex) {
            log.error("Exception occurred running emulator", ex);
            System.exit(1);
        }
    }

    /**
     * Stop controller and host.
     */
    @Override
    public void close() throws Exception {
        inProcPravegaCluster.close();
    }

    /**
     * Start controller and host.
     */
    private void start() throws Exception {
        inProcPravegaCluster.start();
    }

}
