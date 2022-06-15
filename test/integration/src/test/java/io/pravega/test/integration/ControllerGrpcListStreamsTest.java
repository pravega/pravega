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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.test.integration.auth.customplugin.TestAuthHandler;
import io.pravega.test.integration.utils.ClusterWrapper;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import lombok.Cleanup;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests list streams operation of the Controller's gRPC interface.
 */
public class ControllerGrpcListStreamsTest {

    /**
     * This rule makes sure that the tests in this class run in 50 seconds or less.
     */
    @Rule
    public final Timeout globalTimeout = new Timeout(50, TimeUnit.SECONDS);

    @Test
    public void testListStreamsReturnsAllStreamsWhenAuthIsDisabled() {
        // Arrange
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder().build();
        cluster.start();
        String scopeName = "test-scope";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .build();
        this.createStreams(clientConfig, scopeName, Arrays.asList("stream1", "stream2"));

        // Act
        Set<Stream> streams = listStreams(clientConfig, scopeName);

        // Assert
        assertEquals(4, streams.size());
    }

    @Test
    public void testListStreamsReturnsAllStreamsForPrivilegedUserWhenAuthIsEnabled() {
        // Arrange
        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .tokenTtlInSeconds(600)
                .build();
        cluster.start();
        String scopeName = "test-scope";
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();
        this.createStreams(clientConfig, scopeName, Arrays.asList("stream1", "stream2"));

        // Act
        Set<Stream> streams = listStreams(clientConfig, scopeName);

        // Assert
        assertEquals(4, streams.size());
    }

    @Test
    public void testListStreamsReturnsAuthorizedStreamsOnly() {
        // Arrange
        Map<String, String> passwordInputFileEntries = new HashMap<>();
        passwordInputFileEntries.put("admin", "prn::*,READ_UPDATE");
        passwordInputFileEntries.put("user", "prn::/scope:scope1,READ;prn::/scope:scope1/stream:stream1,READ");

        @Cleanup
        ClusterWrapper cluster = ClusterWrapper.builder()
                .authEnabled(true)
                .passwordAuthHandlerEntries(this.preparePasswordInputFileEntries(passwordInputFileEntries))
                .build();

        cluster.start();
        String scopeName = "scope1";

        this.createStreams(ClientConfig.builder()
                        .controllerURI(URI.create(cluster.controllerUri()))
                        .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                        .build(),
                scopeName,
                Arrays.asList("stream1", "stream2", "stream3"));

        // Act
        Set<Stream> streams = listStreams(ClientConfig.builder()
                .controllerURI(URI.create(cluster.controllerUri()))
                .credentials(new DefaultCredentials("1111_aaaa", "user"))
                .build(), scopeName);

        // Assert
        assertEquals(1, streams.size());
    }

    @Test
    public void testListStreamsReturnsAuthorizedStreamsForCustomPlugin() {
        ClusterWrapper cluster = null;
        try {
            // Arrange
            cluster = ClusterWrapper.builder().authEnabled(true).build();
            cluster.start();
            String scopeName = "test-scope";
            this.createStreams(ClientConfig.builder()
                                .controllerURI(URI.create(cluster.controllerUri()))
                                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                .build(),
                               scopeName, Arrays.asList("stream1", "stream2"));

            // Act
            System.setProperty("pravega.client.auth.loadDynamic", "true");
            System.setProperty("pravega.client.auth.method",  TestAuthHandler.METHOD);
            System.setProperty("pravega.client.auth.token", TestAuthHandler.TOKEN);

            Set<Stream> streams = listStreams(ClientConfig.builder()
                    .controllerURI(URI.create(cluster.controllerUri()))
                    .build(), scopeName);

            // Assert
            assertEquals(4, streams.size());
        } finally {
            System.clearProperty("pravega.client.auth.loadDynamic");
            System.clearProperty("pravega.client.auth.method");
            System.clearProperty("pravega.client.auth.token");

            if (cluster != null) {
                cluster.close();
            }
        }
    }

    //region Private methods

    private List<PasswordAuthHandlerInput.Entry> preparePasswordInputFileEntries(Map<String, String> entries) {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");
            List<PasswordAuthHandlerInput.Entry> result = new ArrayList<>();
            entries.forEach((k, v) -> result.add(PasswordAuthHandlerInput.Entry.of(k, encryptedPassword, v)));
            return result;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private void createStreams(ClientConfig clientConfig, String scopeName, List<String> streamNames) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scopeName);
        assertTrue("Failed to create scope", isScopeCreated);

        streamNames.forEach(s -> {
            boolean isStreamCreated =
                    streamManager.createStream(scopeName, s, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
            if (!isStreamCreated) {
                throw new RuntimeException("Failed to create stream: " + s);
            }
        });
    }

    private Set<Stream> listStreams(ClientConfig clientConfig, String scopeName) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        Iterator<Stream> streamsIter = streamManager.listStreams(scopeName);
        Set<Stream> streams = new HashSet<>();
        streamsIter.forEachRemaining(s -> streams.add(s));
        return streams;
    }

    //endregion
}
