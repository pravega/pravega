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
package io.pravega.common.concurrent;

import io.pravega.test.common.IntentionalException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ExecutorServiceFactory} class.
 */
public class ExecutorServiceFactoryTests {
    @After
    public void tearDown() {
        // Invoke GC and finalization - this will make sure that we catch any leftover thread pools
        System.gc();
        System.runFinalization();
    }

    /**
     * Tests {@link ExecutorServiceFactory#getDetectionLevel()}.
     */
    @Test
    public void testGetDetectionLevel() {
        val oldValue = System.getProperty(ExecutorServiceFactory.DETECTION_LEVEL_PROPERTY_NAME);
        try {
            for (val expectedLevel : ExecutorServiceFactory.ThreadLeakDetectionLevel.values()) {
                System.setProperty(ExecutorServiceFactory.DETECTION_LEVEL_PROPERTY_NAME, expectedLevel.name());
                val actualLevel = ExecutorServiceFactory.getDetectionLevel();
                Assert.assertEquals(expectedLevel, actualLevel);
            }

            // Clear it
            System.clearProperty(ExecutorServiceFactory.DETECTION_LEVEL_PROPERTY_NAME);
            Assert.assertEquals(ExecutorServiceFactory.ThreadLeakDetectionLevel.None, ExecutorServiceFactory.getDetectionLevel());
        } finally {
            // Reset the property to its original value.
            if (oldValue == null) {
                System.clearProperty(ExecutorServiceFactory.DETECTION_LEVEL_PROPERTY_NAME);
            } else {
                System.setProperty(ExecutorServiceFactory.DETECTION_LEVEL_PROPERTY_NAME, oldValue);
            }
        }
    }

    @Test
    public void testScheduledThreadPoolLeak() {
        testLeaks(factory -> (ThreadPoolScheduledExecutorService) factory.newScheduledThreadPool(1, "test", 1));
    }

    @Test
    public void testShrinkingThreadPoolLeak() {
        testLeaks(factory -> factory.newShrinkingExecutor(1, 1, "test"));
    }

    private void testLeaks(Function<ExecutorServiceFactory, ExecutorService> newExecutor) {
        for (val level : ExecutorServiceFactory.ThreadLeakDetectionLevel.values()) {
            val invoked = new AtomicBoolean(false);
            Runnable callback = () -> invoked.set(true);
            val factory = new ExecutorServiceFactory(level, callback);

            @Cleanup("shutdownNow")
            val e = newExecutor.apply(factory);
            factory.checkThreadPoolLeak(e, new IntentionalException());

            boolean expectedInvoked = level == ExecutorServiceFactory.ThreadLeakDetectionLevel.Aggressive;
            Assert.assertEquals("Unexpected invocation status for non-shutdown pool.", expectedInvoked, invoked.get());
            invoked.set(false);
            ExecutorServiceHelpers.shutdown(e);
            factory.checkThreadPoolLeak(e, new IntentionalException());
            Assert.assertFalse("Not expecting callback to be invoked for shutdown pool.", invoked.get());
        }
    }
}
