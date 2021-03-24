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
package io.pravega.test.common;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int GLOBAL_BASE_PORT = 11221;
    private static final int GLOBAL_MAX_PORT = 32767;

    private static final AtomicReference<PortRange> PORT_RANGE = new AtomicReference<>();
    private static final AtomicInteger NEXT_PORT = new AtomicInteger();

    // We use a random start position here to avoid ports conflicts when this method is executed from multiple processes
    // in parallel. This is needed since the processes will contend for the same port sequence.
    // private static final AtomicInteger NEXT_PORT = new AtomicInteger(new Random().nextInt(MAX_PORT_COUNT));

    public static synchronized int getAvailableListenPort() {
        if (PORT_RANGE.get() == null) {
            Integer threadId = Integer.getInteger("zookeeper.junit.threadid");
            PORT_RANGE.set(setupPortRange(
                    System.getProperty("test.junit.threads"),
                    threadId != null ? "threadid=" + threadId : System.getProperty("sun.java.command")));
            NEXT_PORT.set(PORT_RANGE.get().getMinimum());
        }
        int candidatePort = NEXT_PORT.get();
        for (;;) {
            ++candidatePort;
            if (candidatePort > PORT_RANGE.get().getMaximum()) {
                candidatePort = PORT_RANGE.get().getMinimum();
            }
            if (candidatePort == NEXT_PORT.get()) {
                throw new IllegalStateException(String.format(
                        "Could not assign port from range %s.  The entire range has been exhausted.",
                        PORT_RANGE.get()));
            }
            try {
                ServerSocket s = new ServerSocket(candidatePort);
                s.close();
                NEXT_PORT.set(candidatePort);
                log.info("Assigned port {} from range {}.", NEXT_PORT.get(), PORT_RANGE.get());
                return NEXT_PORT.get();
            } catch (IOException e) {
                log.debug(
                        "Could not bind to port {} from range {}.  Attempting next port.",
                        candidatePort,
                        PORT_RANGE.get(),
                        e);
            }
        }
    }

    static PortRange setupPortRange(String strProcessCount, String cmdLine) {
        Integer processCount = null;
        if (strProcessCount != null && !strProcessCount.isEmpty()) {
            try {
                processCount = Integer.valueOf(strProcessCount);
            } catch (NumberFormatException e) {
                log.warn("Error parsing test.junit.threads = {}.", strProcessCount, e);
            }
        }

        Integer threadId = null;
        if (processCount != null) {
            if (cmdLine != null && !cmdLine.isEmpty()) {
                Matcher m = Pattern.compile("threadid=(\\d+)").matcher(cmdLine);
                if (m.find()) {
                    try {
                        threadId = Integer.valueOf(m.group(1));
                    } catch (NumberFormatException e) {
                        log.warn("Error parsing threadid from {}.", cmdLine, e);
                    }
                }
            }
        }

        final PortRange newPortRange;
        if (processCount != null && processCount > 1 && threadId != null) {
            // We know the total JUnit process count and this test process's ID.
            // Use these values to calculate the valid range for port assignments
            // within this test process.  We lose a few possible ports to the
            // remainder, but that's acceptable.
            int portRangeSize = (GLOBAL_MAX_PORT - GLOBAL_BASE_PORT) / processCount;
            int minPort = GLOBAL_BASE_PORT + ((threadId - 1) * portRangeSize);
            int maxPort = minPort + portRangeSize - 1;
            newPortRange = new PortRange(minPort, maxPort);
            log.info("Test process {}/{} using ports from {}.", threadId, processCount, newPortRange);
        } else {
            // If running outside the context of Ant or Ant is using a single
            // test process, then use all valid ports.
            newPortRange = new PortRange(GLOBAL_BASE_PORT, GLOBAL_MAX_PORT);
            log.info("Single test process using ports from {}.", newPortRange);
        }

        return newPortRange;
    }

    /**
     * Contains the minimum and maximum (both inclusive) in a range of ports.
     */
    static final class PortRange {

        private final int minimum;
        private final int maximum;

        /**
         * Creates a new PortRange.
         *
         * @param minimum lower bound port number
         * @param maximum upper bound port number
         */
        PortRange(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        /**
         * Returns maximum port in the range.
         *
         * @return maximum
         */
        int getMaximum() {
            return maximum;
        }

        /**
         * Returns minimum port in the range.
         *
         * @return minimum
         */
        int getMinimum() {
            return minimum;
        }

        @Override
        public String toString() {
            return String.format("%d - %d", minimum, maximum);
        }

    }

    /*public synchronized static int getAvailableListenPort() {
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                log.info("Available free port is {}", candidatePort);
                return candidatePort;
            } catch (IOException e) {
                // Do nothing. Try another port.
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }*/

    /**
     * Awaits the given condition to become true.
     *
     * @param condition            A Supplier that indicates when the condition is true. When this happens, this method will return.
     * @param checkFrequencyMillis The number of millis to wait between successive checks of the condition.
     * @param timeoutMillis        The maximum amount of time to wait.
     * @throws TimeoutException If the condition was not met during the allotted time.
     */
    @SneakyThrows(InterruptedException.class)
    public static void await(Supplier<Boolean> condition, int checkFrequencyMillis, long timeoutMillis) throws TimeoutException {
        long remainingMillis = timeoutMillis;
        while (!condition.get() && remainingMillis > 0) {
            Thread.sleep(checkFrequencyMillis);
            remainingMillis -= checkFrequencyMillis;
        }

        if (!condition.get() && remainingMillis <= 0) {
            throw new TimeoutException("Timeout expired prior to the condition becoming true.");
        }
    }

    /**
     * Awaits the given condition to become true, where condition could be non-repeatable.
     *
     * @param condition            A Supplier that indicates when the condition is true. When this happens, this method will return.
     * @param checkFrequencyMillis The number of millis to wait between successive checks of the condition.
     * @param timeoutMillis        The maximum amount of time to wait.
     * @throws TimeoutException If the condition was not met during the allotted time.
     */
    @SneakyThrows(InterruptedException.class)
    public static void awaitException(Supplier<Boolean> condition, int checkFrequencyMillis, long timeoutMillis) throws TimeoutException {
        long remainingMillis = timeoutMillis;
        boolean result = false;
        while (!(result = condition.get()) && remainingMillis > 0) {
            Thread.sleep(checkFrequencyMillis);
            remainingMillis -= checkFrequencyMillis;
        }

        if (!result && remainingMillis <= 0) {
            throw new TimeoutException("Timeout expired prior to the condition becoming true.");
        }
    }

    /**
     * Generates an auth token using the Basic authentication scheme.
     * @param username the username to use.
     * @param password the password to use.
     * @return an en encoded token.
     */
    public static String basicAuthToken(String username, String password) {
        String decoded = String.format("%s:%s", username, password);
        String encoded = Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    /**
     * Replace final static field for unit testing purpose.
     *
     * @param field the final static field to be replaced
     * @param newValue the object to replace the existing final static field
     * @throws Exception when the operation cannot be completed
     */
    public static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }

    /**
     * A no-op {@link java.util.function.Consumer<T>} that does nothing.
     *
     * @param ignored Arg.
     * @param <T>     Type.
     */
    public static <T> void doNothing(T ignored) {
        // Does nothing.
    }

}
