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
package io.pravega.test.integration;

import io.grpc.StatusRuntimeException;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.utils.ClusterWrapper;
import io.pravega.test.integration.utils.TestUtils;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ReadWithReadPermissionsTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    /**
     * This test verifies that data can be read from a stream using read-only permissions, if the system is configured
     * to allow writes to internal streams with read-only permissions.
     */
    @SneakyThrows
    @Test
    public void readWithReadOnlyPermissions() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();

        // This `creator` account is used to create the objects
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");

        // This `reader` account is used to read back from the stream.
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ"
        ));
        writeThenReadDataBack(passwordInputFileEntries, true);
    }

    @Test
    public void readsRequireWritePermissionsOnRgWhenConfigIsFalse() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ_UPDATE",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ"
        ));
        AssertExtensions.assertThrows(StatusRuntimeException.class,
                () -> writeThenReadDataBack(passwordInputFileEntries, false));
    }

    @SneakyThrows
    @Test
    public void readsWorkWithWritePermissionsWhenConfigIsFalse() {
        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("creator", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("reader", String.join(";",
                "prn::/scope:MarketData,READ_UPDATE",
                "prn::/scope:MarketData/stream:StockPriceUpdates,READ",
                "prn::/scope:MarketData/reader-group:PriceChangeCalculator,READ_UPDATE"
        ));
        writeThenReadDataBack(passwordInputFileEntries, false);
    }

    @SneakyThrows
    private void writeThenReadDataBack(Map<String, String> passwordInputFileEntries,
                                       boolean writeToInternalStreamsWithReadPermission) {
        final String scopeName = "MarketData";
        final String streamName = "StockPriceUpdates";
        final String readerGroupName = "PriceChangeCalculator";
        final String message = "SCRIP:DELL,EXCHANGE:NYSE,PRICE=100";
        final String pwd = "secret-password";

        // Setup the cluster and create the objects
        @Cleanup
        final ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tokenSigningKeyBasis("secret").tokenTtlInSeconds(600)
                .rgWritesWithReadPermEnabled(writeToInternalStreamsWithReadPermission)
                .passwordAuthHandlerEntries(TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, pwd))
                .build();
        cluster.start();

        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(pwd, "creator"))
                .build();
        TestUtils.createScopeAndStreams(writerClientConfig, scopeName, Arrays.asList(streamName));
        TestUtils.writeDataToStream(scopeName, streamName, message, writerClientConfig);

        // Now, read data back using the reader account.

        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(pwd, "reader"))
                .build();

        String readMessage = TestUtils.readNextEventMessage(scopeName, streamName, readerClientConfig, readerGroupName);
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }

    @Test
    public void readsFromADifferentScopeTest() {
        String marketDataWriter = "writer";
        String marketDataReader = "reader";
        String password = "test-password";
        String marketDataScope = "marketdata";
        String computeScope = "compute";
        String stream1 = "stream1";

        final Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put(marketDataWriter, String.join(";",
                "prn::/,READ_UPDATE", // Allows user to create the "marketdata" scope, for this test
                "prn::/scope:marketdata,READ_UPDATE", // Allows user to create stream (and other scope children)
                "prn::/scope:marketdata/*,READ_UPDATE"  // Provides user all access to child objects of the "marketdata" scope
        ));

        passwordInputFileEntries.put(marketDataReader, String.join(";",
                "prn::/,READ_UPDATE", // Allows use to create the "compute" home scope
                "prn::/scope:compute,READ_UPDATE", // Allows user to create reader-group under its home scope
                "prn::/scope:compute/*,READ_UPDATE", // Provides user all access to child objects of the "compute" scope
                "prn::/scope:marketdata/stream:stream1,READ" // Provides use read access to the "marketdata/stream1" stream.
        ));

        // Setup and run the servers
        @Cleanup
        final ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tokenSigningKeyBasis("secret").tokenTtlInSeconds(600)
                .rgWritesWithReadPermEnabled(false)
                .passwordAuthHandlerEntries(
                        TestUtils.preparePasswordInputFileEntries(passwordInputFileEntries, password))
                .build();
        cluster.start();

        // Prepare a client config for the `marketDataWriter`, whose home scope is "marketdata"
        final ClientConfig writerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(password, marketDataWriter))
                .build();

        // Create scope/stream `marketdata/stream1`
        TestUtils.createScopeAndStreams(writerClientConfig, marketDataScope, Arrays.asList(stream1));

        // Write a message to stream `marketdata/stream1`
        TestUtils.writeDataToStream(marketDataScope, stream1, "test message", writerClientConfig);

        // Prepare a client config for `marketDataReader`, whose home scope is "compute"
        ClientConfig readerClientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials(password, marketDataReader))
                .build();

        // Create scope `compute` (without any streams)
        TestUtils.createScopeAndStreams(readerClientConfig, computeScope, new ArrayList<>());

        // Create a reader group config that enables a user to read data from `marketdata/stream1`
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(marketDataScope, stream1))
                .disableAutomaticCheckpoints()
                .build();

        // Create a reader-group for user `marketDataReader` in `compute` scope, which is its home scope.
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(computeScope, readerClientConfig);
        readerGroupManager.createReaderGroup("testRg", readerGroupConfig);

        @Cleanup
        EventStreamClientFactory readerClientFactory =
                EventStreamClientFactory.withScope(computeScope, readerClientConfig);
        @Cleanup
        EventStreamReader<String> reader = readerClientFactory.createReader(
                "readerId", "testRg",
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());
        String readMessage = reader.readNextEvent(5000).getEvent();

        assertEquals("test message", readMessage);
    }
}
