/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.integrationtests;

import com.emc.pravega.common.metrics.MetricsConfig;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.StatsProvider;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.testcommon.TestUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileReader;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Integration test for Pravega Metrics.
 * The Metrics framework using CSVReporter reports the metrics locally to temporary folder.
 * Verify using the Apache Commons CSV parser that the reported values are correct.
 */
@Slf4j
public class MetricsTest {
    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream";
    private static final String STREAM2 = "testStream2";
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private StreamConfiguration streamConfiguration = null;
    private StreamConfiguration streamConfiguration2 = null;
    private ServiceBuilderConfig builderConfig;
    private StatsProvider statsProvider;

    @Before
    public void setUp() throws Exception {
        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        try {
            // 1. Start ZK
            this.zkTestServer = new TestingServer();

            // 2. Start Metrics service.
            log.info("Initializing metrics provider ...");
            builderConfig = ServiceBuilderConfig.builder().include(ServiceConfig.builder().
                    with(ServiceConfig.LISTENING_PORT, servicePort).with(ServiceConfig.CONTAINER_COUNT, 1)).build();
            MetricsProvider.initialize(builderConfig.getConfig(MetricsConfig::builder));
            statsProvider = MetricsProvider.getMetricsProvider();
            statsProvider.start();

            // 3. Start Pravega service.
            ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(builderConfig);
            serviceBuilder.initialize().get();
            StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

            this.server = new PravegaConnectionListener(false, servicePort, store);
            this.server.startListening();

            // 4. Start controller
            this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, true,
                    controllerPort, serviceHost, servicePort, containerCount);
            this.controllerWrapper.awaitRunning();
            this.controller = controllerWrapper.getController();
            this.streamConfiguration = StreamConfiguration.builder()
                    .scope(SCOPE)
                    .streamName(STREAM)
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            this.streamConfiguration2 = StreamConfiguration.builder()
                    .scope(SCOPE)
                    .streamName(STREAM2)
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
        } catch (Exception e) {
            log.error("Error during setup", e);
            throw e;
        }
    }

    @After
    public void tearDown() {
        try {
            if (this.controllerWrapper != null) {
                this.controllerWrapper.close();
                this.controllerWrapper = null;
            }
            if (this.server != null) {
                this.server.close();
                this.server = null;
            }
            if (this.zkTestServer != null) {
                this.zkTestServer.close();
                this.zkTestServer = null;
            }
            if (this.statsProvider != null) {
                this.statsProvider.close();
                this.statsProvider = null;
            }
        } catch (Exception e) {
            log.warn("Exception while tearing down", e);
        }
    }

    @Test(timeout = 120000)
    public void metricsTest() throws Exception {
        final String csvFolderPath = MetricsConfig.CSV_ENDPOINT.getDefaultValue() + "/pravega/";
        CSVParser csvFileParser;
        String filePath;
        List<CSVRecord> recordList;
        CSVRecord csvRecord;

        assertTrue(controller.createScope(SCOPE).join());
        assertTrue(controller.createStream(streamConfiguration).join());
        assertTrue(controller.createStream(streamConfiguration2).join());

        // Test the controller.stream_deleted metric
        // Get the last record and verify the count for deleted streams is greater that 0
        assertTrue(controller.sealStream(SCOPE, STREAM2).join());
        assertTrue(controller.deleteStream(SCOPE, STREAM2).join());
        filePath = new StringBuilder(csvFolderPath).append("controller.stream_deleted.csv").toString();
        @Cleanup
        FileReader fileReader = new FileReader(filePath);
        csvFileParser = CSVFormat.DEFAULT.parse(fileReader);
        recordList = csvFileParser.getRecords();
        csvRecord = recordList.get(recordList.size() - 1);
        assertTrue("Number of deleted streams", Integer.parseInt(csvRecord.get(1)) > 0);
        csvFileParser.close();
        fileReader.close();

        // Test DYNAMIC.$scope.$stream.transactions_timedout metric
        // Get the last record and verify the count for timed out transactions is greater that 0
        controller.createTransaction(new StreamImpl(SCOPE, STREAM), 1, 2, 29000);
        Thread.sleep(5000);
        filePath = new StringBuilder(csvFolderPath).append("DYNAMIC.").append(SCOPE).append(".")
                .append(STREAM).append(".transactions_timedout.Meter.csv").toString();
        fileReader = new FileReader(filePath);
        csvFileParser = CSVFormat.DEFAULT.parse(fileReader);
        recordList = csvFileParser.getRecords();
        csvRecord = recordList.get(recordList.size() - 1);
        assertTrue("Number of timed-out transactions", Integer.parseInt(csvRecord.get(1)) > 0);
        csvFileParser.close();
        fileReader.close();
    }
}
