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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ConnectionTracker} class.
 */
public class ConnectionTrackerTests {

    @Test
    public void testConstructor() {
        AssertExtensions.assertThrows(
                "Constructor accepted SingleConnectionMaxLimit under LOW_WATERMARK threshold.",
                () -> new ConnectionTracker(ConnectionTracker.LOW_WATERMARK + 1, ConnectionTracker.LOW_WATERMARK - 1),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "Constructor accepted AllConnectionsMaxLimit under LOW_WATERMARK threshold.",
                () -> new ConnectionTracker(ConnectionTracker.LOW_WATERMARK - 1, ConnectionTracker.LOW_WATERMARK - 2),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "Constructor accepted SingleConnectionMaxLimit > AllConnectionsMaxLimit.",
                () -> new ConnectionTracker(ConnectionTracker.LOW_WATERMARK + 2, ConnectionTracker.LOW_WATERMARK + 3),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Verifies that {@link ConnectionTracker#getTotalOutstanding()} is calculated properly.
     */
    @Test
    public void testTotalOutStanding() {
        val c = new MockConnection();
        val t = new ConnectionTracker();
        t.updateOutstandingBytes(c, 10, 10);
        Assert.assertEquals("Unexpected value from getTotalOutstanding.", 10, t.getTotalOutstanding());
        t.updateOutstandingBytes(c, 1, 2);
        Assert.assertEquals("Unexpected value from getTotalOutstanding.", 11, t.getTotalOutstanding());
        t.updateOutstandingBytes(c, -100, 0);
        Assert.assertEquals("Unexpected value from getTotalOutstanding(low-bound).", 0, t.getTotalOutstanding());
    }

    /**
     * Verifies various scenarios for {@link ConnectionTracker#updateOutstandingBytes}.
     */
    @Test
    public void testAdjustOutstandingBytes() {
        val allLimit = ConnectionTracker.LOW_WATERMARK * 4;
        val singleLimit = ConnectionTracker.LOW_WATERMARK * 2;
        val t = new ConnectionTracker(allLimit, singleLimit);
        val c = new MockConnection();

        // A connection increased, but it's under both the per-connection limit and total limit.
        t.updateOutstandingBytes(c, singleLimit - 2, singleLimit - 2);
        Assert.assertFalse("Not expecting a connection pause when under the limit.", c.isPaused());

        // Single connection cannot exceed its limit.
        t.updateOutstandingBytes(c, singleLimit - 1, singleLimit + 1);
        Assert.assertTrue("Expected a connection pause when connection over limit.", c.isPaused());

        // Increase a connection by 2. This still keeps the total under allLimit, but the per-connection quota would
        // have been exceeded. The only condition allowing this to stay alive is that it's below the LOW_WATERMARK.
        t.updateOutstandingBytes(c, 2, 2);
        Assert.assertFalse("Not expected a connection pause when connection under LOW_WATERMARK.", c.isPaused());

        // Increase a connection by 2. This should put the total limit above the absolute threshold, so it should be rejected.
        t.updateOutstandingBytes(c, 2, 2);
        Assert.assertTrue("Expected a connection pause when total is above limit.", c.isPaused());

        // Decrease a connection by a good amount. This should still be off because the cumulative total is too high.
        t.updateOutstandingBytes(c, -ConnectionTracker.LOW_WATERMARK, ConnectionTracker.LOW_WATERMARK + 1);
        Assert.assertTrue("Expected a connection pause connection is over limit.", c.isPaused());

        // Repeat the last step. This should be OK now, because we've reduced the total to a low-enough value
        t.updateOutstandingBytes(c, -ConnectionTracker.LOW_WATERMARK, ConnectionTracker.LOW_WATERMARK + 1);
        Assert.assertFalse("Not expected a connection pause when total is reduced below limit.", c.isPaused());
    }

    /**
     * Verifies {@link TrackedConnection}.
     */
    @Test
    public void testTrackedConnection() {
        val allLimit = ConnectionTracker.LOW_WATERMARK * 4;
        val singleLimit = ConnectionTracker.LOW_WATERMARK * 2;
        val baseTracker = new ConnectionTracker(allLimit, singleLimit);
        val c1 = new MockConnection();
        @Cleanup
        val t1 = new TrackedConnection(c1, baseTracker);
        val c2 = new MockConnection();
        @Cleanup
        val t2 = new TrackedConnection(c2, baseTracker);

        // A connection increased, but it's under both the per-connection limit and total limit.
        t1.adjustOutstandingBytes(singleLimit - 2);
        Assert.assertFalse("Not expecting a connection pause when under the limit.", c1.isPaused());

        // Single connection cannot exceed its limit.
        t1.adjustOutstandingBytes(3);
        Assert.assertTrue("Expected a connection pause when connection over limit.", c1.isPaused());

        // Increase second connection by the max it can be increased. This should pause the connection.
        t2.adjustOutstandingBytes(singleLimit / 2 - 1);
        Assert.assertFalse("Not expected a connection pause when total is below limit.", c2.isPaused());
        t2.adjustOutstandingBytes(1);
        Assert.assertTrue("Expected a connection pause when total is above limit.", c2.isPaused());
        Assert.assertEquals(singleLimit + 1 + singleLimit / 2, baseTracker.getTotalOutstanding());

        // Decrease the last increase for connection 1. Since the are multiple connections, the overall per-connection
        // max threshold decreased, so we cannot yet resume it.
        t1.adjustOutstandingBytes(-3);
        Assert.assertTrue("Expected a connection pause when connection was over limit (near total capacity).", c1.isPaused());

        t1.adjustOutstandingBytes(-singleLimit / 2);
        Assert.assertFalse("Expected a connection resume when connection dropped below limit .", c1.isPaused());

        t2.adjustOutstandingBytes(-1);
        Assert.assertFalse("Expected a connection resume when connection dropped below limit .", c2.isPaused());
    }

    private static class MockConnection implements ServerConnection {
        @Getter
        private boolean paused = false;

        @Override
        public void pauseReading() {
            this.paused = true;
        }

        @Override
        public void resumeReading() {
            this.paused = false;
        }

        @Override
        public void send(WireCommand cmd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setRequestProcessor(RequestProcessor cp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        @Override
        public boolean isClosed() {
            throw new UnsupportedOperationException();
        }
    }
}
