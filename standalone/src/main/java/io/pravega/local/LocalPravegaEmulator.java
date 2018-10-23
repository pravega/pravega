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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class LocalPravegaEmulator implements AutoCloseable {

    private int zkPort;
    private int controllerPort;
    private int segmentStorePort;
    private int restServerPort;
    private boolean enableRestServer;
    private boolean enableAuth;
    private boolean enableTls;
    private String certFile;
    private String passwd;
    private String userName;
    private String passwdFile;
    private String keyFile;
    private String jksKeyFile;
    private String jksTrustFile;
    private String keyPasswordFile;

    @Getter
    private final InProcPravegaCluster inProcPravegaCluster;

    public static final class LocalPravegaEmulatorBuilder {
        public LocalPravegaEmulator build() {
            this.inProcPravegaCluster = InProcPravegaCluster
                    .builder()
                    .isInProcZK(true)
                    .secureZK(enableTls)
                    .zkUrl("localhost:" + zkPort)
                    .zkPort(zkPort)
                    .isInMemStorage(true)
                    .isInProcController(true)
                    .controllerCount(1)
                    .isInProcSegmentStore(true)
                    .segmentStoreCount(1)
                    .containerCount(4)
                    .restServerPort(restServerPort)
                    .enableRestServer(enableRestServer)
                    .enableMetrics(false)
                    .enableAuth(enableAuth)
                    .enableTls(enableTls)
                    .certFile(certFile)
                    .keyFile(keyFile)
                    .jksKeyFile(jksKeyFile)
                    .jksTrustFile(jksTrustFile)
                    .keyPasswordFile(keyPasswordFile)
                    .passwdFile(passwdFile)
                    .userName(userName)
                    .passwd(passwd)
                    .build();
            this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
            this.inProcPravegaCluster.setSegmentStorePorts(new int[]{segmentStorePort});
            return new LocalPravegaEmulator(zkPort, controllerPort, segmentStorePort, restServerPort, enableRestServer,
                    enableAuth, enableTls, certFile, passwd, userName, passwdFile, keyFile, jksKeyFile, jksTrustFile, keyPasswordFile,
                    inProcPravegaCluster);
        }
    }

    public static void main(String[] args) {
        try {
            ServiceBuilderConfig config = ServiceBuilderConfig
                    .builder()
                    .include(System.getProperty(SingleNodeConfig.PROPERTY_FILE, "./config/standalone-config.properties"))
                    .include(System.getProperties())
                    .build();
            SingleNodeConfig conf = config.getConfig(SingleNodeConfig::builder);

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder()
                    .controllerPort(conf.getControllerPort())
                    .segmentStorePort(conf.getSegmentStorePort())
                    .zkPort(conf.getZkPort())
                    .restServerPort(conf.getRestServerPort())
                    .enableRestServer(conf.isEnableRestServer())
                    .enableAuth(conf.isEnableAuth())
                    .enableTls(conf.isEnableTls())
                    .certFile(conf.getCertFile())
                    .keyFile(conf.getKeyFile())
                    .passwdFile(conf.getPasswdFile())
                    .userName(conf.getUserName())
                    .passwd(conf.getPasswd())
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
     * @throws Exception passes on the exception thrown by `inProcPravegaCluster`
     */
    void start() throws Exception {
        inProcPravegaCluster.start();
    }

}
