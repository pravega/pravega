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
package io.pravega.segmentstore.storage;

import io.pravega.test.common.IntentionalException;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ThrottlerSourceListenerCollection} class.
 */
public class ThrottlerSourceListenerCollectionTests {
    /**
     * Tests {@link ThrottlerSourceListenerCollection#register} and {@link ThrottlerSourceListenerCollection#notifySourceChanged()}.
     */
    @Test
    public void testRegisterNotify() {
        val c = new ThrottlerSourceListenerCollection();
        val l1 = new TestListener();
        val l2 = new TestListener();
        val l3 = new TestListener();
        l3.closed = true;
        c.register(l1);
        c.register(l2);
        c.register(l3);
        Assert.assertEquals("Not expecting closed listener to be added.", 2, c.getListenerCount());

        l2.setClosed(true);
        c.notifySourceChanged();
        Assert.assertEquals("Expected listener to be invoked.", 1, l1.getCallCount());
        Assert.assertEquals("Not expected closed listener to be invoked.", 0, l2.getCallCount());
        c.register(l2); // This should have no effect.
        Assert.assertEquals("Expected closed listener to be removed.", 1, c.getListenerCount());

        // Now verify with errors.
        c.register(new ThrottleSourceListener() {
            @Override
            public void notifyThrottleSourceChanged() {
                throw new IntentionalException();
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });

        c.notifySourceChanged(); // This should not throw.
        Assert.assertEquals("Expected cleanup listener to be invoked the second time.", 2, l1.getCallCount());
        Assert.assertEquals("Not expected removed listeners to be invoked.", 0, l2.getCallCount() + l3.getCallCount());
    }

    private static class TestListener implements ThrottleSourceListener {
        @Getter
        private int callCount = 0;
        @Setter
        @Getter
        private boolean closed;

        @Override
        public void notifyThrottleSourceChanged() {
            this.callCount++;
        }
    }
}
