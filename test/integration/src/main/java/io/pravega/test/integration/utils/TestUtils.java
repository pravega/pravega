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
package io.pravega.test.integration.utils;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * A helper class with general-purpose utility methods for integration tests.
 */
@Slf4j
public class TestUtils {

    /**
     * Creates the specified {@code scope} and {@code streams}, using the specified {@code clientConfig}.
     *
     * Note: This method creates the streams using a scaling policy with a fixed number of segments (one each).
     *
     * @param clientConfig the {@link ClientConfig} to use for connecting to the server
     * @param scope the scope
     * @param streams the streams
     * @return whether all the objects (scope and each of the streams) were newly created. Returns {@code false}, if
     *         any of those objects were already present.
     */
    public static boolean createScopeAndStreams(ClientConfig clientConfig, String scope, List<String> streams) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean result = streamManager.createScope(scope);
        for (String stream: streams) {
            boolean isStreamCreated = streamManager.createStream(scope, stream,
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
            result = result && !isStreamCreated ? false : true;
        }
        return result;
    }

    /**
     * Write the {@code message} to the specified {@code scope}/{@code stream}, using the
     * provided {@writerClientConfig}.
     *
     * @param scope the scope
     * @param stream the stream
     * @param message the message to write. If it is null, a default message will be used.
     * @param writerClientConfig the {@link ClientConfig} object to use to connect to the server
     * @throws NullPointerException if {@code scope} or {@code stream} or {@writerClientConfig} is null
     * @throws RuntimeException if any exception is thrown by the client
     */
    @SneakyThrows
    public static void writeDataToStream(String scope, String stream, String message, ClientConfig writerClientConfig) {
        writeDataToStream(scope, stream, message, 1, writerClientConfig);
    }

    /**
     * Write the specified number of messages to the specified {@code scope}/{@code stream}, using the
     * provided {@writerClientConfig}.
     *
     * @param scope the scope
     * @param stream the stream
     * @param message the event message to write. If it is null, a default message will be used.
     * @param numMessages the number of event messages to write
     * @param writerClientConfig the {@link ClientConfig} object to use to connect to the server
     * @throws NullPointerException if {@code scope} or {@code stream} or {@writerClientConfig} is null
     * @throws IllegalArgumentException if {@code numMessages} &lt; 1
     * @throws RuntimeException if any exception is thrown by the client
     */
    public static void writeDataToStream(@NonNull String scope, @NonNull String stream, String message, int numMessages,
                                         @NonNull ClientConfig writerClientConfig) {
        Preconditions.checkArgument(numMessages > 0);
        if (message == null) {
            message = "Test message";
        }
        @Cleanup final EventStreamClientFactory writerClientFactory = EventStreamClientFactory.withScope(scope,
                writerClientConfig);

        @Cleanup final EventStreamWriter<String> writer = writerClientFactory.createEventWriter(stream,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < numMessages; i++) {
            writer.writeEvent(message);
        }
        writer.flush();
        log.info("Wrote {} message(s) to the stream {}/{}", numMessages, scope, stream);
    }

    /**
     * Returns the next unread message from the specified {@code scope}/{@code stream}.
     *
     * @param scope the scope
     * @param stream the stream
     * @param readerClientConfig the {@link ClientConfig} object to use to connect to the server
     * @param readerGroup the name of the reader group application
     * @return the event message
     */
    public static String readNextEventMessage(String scope, String stream, ClientConfig readerClientConfig,
                                              String readerGroup) {
        List<String> messages = readNextEventMessages(scope, stream, 1, readerClientConfig, readerGroup);
        if (messages == null || messages.size() == 0) {
            return null;
        } else {
            return messages.get(0);
        }
    }

    /**
     * Returns the specified number of unread messages from the given {@code scope}/{@code stream}.
     *
     * @param scope the scope
     * @param stream the stream
     * @param numMessages the number of event messages to read
     * @param readerClientConfig the {@link ClientConfig} object to use to connect to the server
     * @param readerGroup the name of the reader group application
     * @return the event messages
     * @throws NullPointerException if {@code scope} or {@code stream} or {@writerClientConfig} is null
     * @throws IllegalArgumentException if {@code numMessages} &lt; 1
     * @throws RuntimeException if any exception is thrown by the client
     */
    public static List<String> readNextEventMessages(@NonNull String scope, @NonNull String stream, int numMessages,
                                                     @NonNull ClientConfig readerClientConfig, @NonNull String readerGroup) {
        Preconditions.checkArgument(numMessages > 0);

        @Cleanup
        EventStreamClientFactory readerClientFactory = EventStreamClientFactory.withScope(scope, readerClientConfig);
        log.debug("Created the readerClientFactory");

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, stream))
                .disableAutomaticCheckpoints()
                .build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, readerClientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        log.debug("Created reader group with name {}", readerGroup);

        @Cleanup
        EventStreamReader<String> reader = readerClientFactory.createReader(
                "readerId", readerGroup,
                new JavaSerializer<String>(), ReaderConfig.builder().initialAllocationDelay(0).build());
        log.debug("Created an event reader");

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        List<String> result = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            result.add(reader.readNextEvent(20000).getEvent());
        }
        log.info("Done reading {} events", numMessages);
        return result;
    }

    /**
     * Prepares a list of password auth handler user account database file entries. The
     * {@link ClusterWrapper} accepts entries in the returned format.
     *
     * @param entries ACLs by user
     * @param password the plaintext password for each user
     * @return Password auth handler user account database entries
     */
    @SneakyThrows
    public static List<PasswordAuthHandlerInput.Entry> preparePasswordInputFileEntries(
            Map<String, String> entries, String password) {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        String encryptedPassword = passwordProcessor.encryptPassword(password);
        List<PasswordAuthHandlerInput.Entry> result = new ArrayList<>();
        entries.forEach((k, v) -> result.add(PasswordAuthHandlerInput.Entry.of(k, encryptedPassword, v)));
        return result;
    }

    /**
     * Returns the relative path to `pravega/config` source directory from integration tests.
     *
     * @return the path
     */
    public static String pathToConfig() {
        return "../../config/";
    }
}
