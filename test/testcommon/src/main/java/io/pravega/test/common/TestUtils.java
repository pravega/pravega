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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int BASE_PORT = 32768;
    private static final int MAX_PORT_COUNT = 28233;
    private static final Random RAND = new Random();

    // We use a random start position here to avoid ports conflicts when this method is executed from multiple processes
    // in parallel. This is needed since the processes will contend for the same port sequence.
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(RAND.nextInt());

    /**
     * A helper method to get a random free TCP port.
     *
     * @return free port.
     */
    public synchronized static int getAvailableListenPort() {
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                log.info("Available free port is {}", candidatePort);
                return candidatePort;
            } catch (IOException e) {
                // Do nothing. Try another port.
                NEXT_PORT.addAndGet(RAND.nextInt());
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }

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
     * Awaits the return value of the given expression to be equal to the expected value.
     * @param <T>                  The type of the value to compare.
     * @param expected             The expected return value.
     * @param eval                 The function to test
     * @param checkFrequencyMillis The number of millis to wait between successive checks of the condition.
     * @param timeoutMillis        The maximum amount of time to wait.
     * @throws Exception           If there is an assertion error, and exception from `eval`, or the thread is interrupted.
     * @throws TimeoutException    If the condition was not met during the allotted time.
     * @return the value returned by the function.
     */
    public static <T> T awaitEvaluateExpr(T expected, Callable<T> eval, int checkFrequencyMillis, long timeoutMillis) throws Exception, TimeoutException {

        T result = null;
        Supplier<Boolean> condition = null;
        long remainingMillis = timeoutMillis;

        while (condition == null || !condition.get() && remainingMillis > 0) {
            try {
                result = eval.call();
                T finalResult = result;
                condition = () -> (expected == null && finalResult == null)
                        || (expected != null && expected.equals(finalResult));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            Thread.sleep(checkFrequencyMillis);
            remainingMillis -= checkFrequencyMillis;
        }

        if (condition == null || !condition.get() && remainingMillis <= 0) {
            throw new TimeoutException("Timeout expired prior to the condition becoming true. Expected value: " + expected + " observed: " + result);
        }

        return result;
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
