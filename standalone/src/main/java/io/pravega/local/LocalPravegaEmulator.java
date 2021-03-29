/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.local;

import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Main entry point for Integration tests that need an in-process Pravega cluster.
 * This class is intended to be used both by internal test suites
 * and by test suites of applications that want to run tests against
 * a real Pravega cluster.
 */
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
    private boolean enableTlsReload;
    private String jksKeyFile;
    private String jksTrustFile;
    private String keyPasswordFile;
    private boolean enableMetrics;
    private boolean enableInfluxDB;
    private int metricsReportInterval;

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
                    .containerCount(1)
                    .restServerPort(restServerPort)
                    .enableRestServer(enableRestServer)
                    .enableMetrics(false)
                    .enableAuth(enableAuth)
                    .enableTls(enableTls)
                    .certFile(certFile)
                    .keyFile(keyFile)
                    .enableTlsReload(enableTlsReload)
                    .jksKeyFile(jksKeyFile)
                    .jksTrustFile(jksTrustFile)
                    .keyPasswordFile(keyPasswordFile)
                    .passwdFile(passwdFile)
                    .userName(userName)
                    .passwd(passwd)
                    .enableMetrics(enableMetrics)
                    .enableInfluxDB(enableInfluxDB)
                    .metricsReportInterval(metricsReportInterval)
                    .build();
            this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
            this.inProcPravegaCluster.setSegmentStorePorts(new int[]{segmentStorePort});
            return new LocalPravegaEmulator(zkPort, controllerPort, segmentStorePort, restServerPort, enableRestServer,
                    enableAuth, enableTls, certFile, passwd, userName, passwdFile, keyFile, enableTlsReload,
                    jksKeyFile, jksTrustFile, keyPasswordFile, enableMetrics, enableInfluxDB, metricsReportInterval, inProcPravegaCluster);
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
                    .enableMetrics(conf.isEnableMetrics())
                    .enableInfluxDB(conf.isEnableInfluxDB())
                    .metricsReportInterval(conf.getMetricsReportInterval())
                    .certFile(conf.getCertFile())
                    .keyFile(conf.getKeyFile())
                    .enableTlsReload(conf.isEnableSegmentStoreTlsReload())
                    .jksKeyFile(conf.getKeyStoreJKS())
                    .jksTrustFile(conf.getTrustStoreJKS())
                    .keyPasswordFile(conf.getKeyStoreJKSPasswordFile())
                    .passwdFile(conf.getPasswdFile())
                    .userName(conf.getUserName())
                    .passwd(conf.getPasswd())
                    .build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        log.info("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                        log.warn("Exception running local Pravega emulator: " + e.getMessage());
                    }
                }
            });

            log.info("Starting Pravega Emulator with ports: ZK port {}, controllerPort {}, SegmentStorePort {}",
                    conf.getZkPort(), conf.getControllerPort(), conf.getSegmentStorePort());
            localPravega.start();
            log.info("");
            log.info("Pravega Sandbox is running locally now. You could access it at {}:{}.", "127.0.0.1", conf.getControllerPort());
            log.info("For more detailed logs, see: {}/{}", System.getProperty("user.dir"), "standalone/standalone.log");
            log.info("");
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
    public void start() throws Exception {
        log.info("\n{}", inProcPravegaCluster.print());
        inProcPravegaCluster.start();
    }

}
