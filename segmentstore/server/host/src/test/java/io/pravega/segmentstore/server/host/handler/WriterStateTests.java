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

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link WriterState} class.
 */
public class WriterStateTests {
    /**
     * Tests {@link WriterState#beginAppend} and {@link WriterState#appendSuccessful}.
     */
    @Test
    public void testAppendSuccessful() {
        // Begin with recording 3 Events.
        final long initialEventNumber = 1;
        val ws = new WriterState(initialEventNumber);
        long event1 = initialEventNumber + 1;
        val begin1 = ws.beginAppend(event1);
        Assert.assertEquals("beginAppend(1) returned unexpected LastStoredEventNumber.", initialEventNumber, begin1);
        long event2 = event1 + 1;
        val begin2 = ws.beginAppend(event2);
        Assert.assertEquals("beginAppend(2) returned unexpected LastStoredEventNumber.", event1, begin2);
        long event3 = event2 + 1;
        val begin3 = ws.beginAppend(event3);
        Assert.assertEquals("beginAppend(3) returned unexpected LastStoredEventNumber.", event2, begin3);

        // Ack Event 1. The Previous Last Ack should be the initial event number.
        val ack1 = ws.appendSuccessful(event1);
        Assert.assertEquals("appendSuccessful(1) returned unexpected PreviousLastAcked.", initialEventNumber, ack1);

        // Ack Event 3 before Event 2. The previous last ack must be 1 (since that's our last ack).
        val ack3 = ws.appendSuccessful(event3);
        Assert.assertEquals("appendSuccessful(3) returned unexpected PreviousLastAcked.", event1, ack3);

        // Ack Event 2. The previous ack was 3, so return that.
        val ack2 = ws.appendSuccessful(event2);
        Assert.assertEquals("appendSuccessful(2) returned unexpected PreviousLastAcked.", event3, ack2);
        Assert.assertEquals(WriterState.NO_FAILED_EVENT_NUMBER, ws.getLowestFailedEventNumber());
    }

    /**
     * Tests {@link WriterState#beginAppend} and {@link WriterState#appendSuccessful} and EventSizeForAppend.
     */
    @Test
    public void testEventSizeForAppend() {
        // Begin with recording 3 Events.
        final long initialEventNumber = 1;
        val ws = new WriterState(initialEventNumber, 24L);
        long event1 = initialEventNumber + 1;
        val begin1 = ws.beginAppend(event1);
        Assert.assertEquals("beginAppend(1) returned unexpected LastStoredEventNumber.", initialEventNumber, begin1);
        long event2 = event1 + 1;
        val begin2 = ws.beginAppend(event2);
        Assert.assertEquals("beginAppend(2) returned unexpected LastStoredEventNumber.", event1, begin2);
        long event3 = event2 + 1;
        val begin3 = ws.beginAppend(event3);
        Assert.assertEquals("beginAppend(3) returned unexpected LastStoredEventNumber.", event2, begin3);

        // Ack Event 1. The Previous Last Ack should be the initial event number.
        val ack1 = ws.appendSuccessful(event1);
        Assert.assertEquals("appendSuccessful(1) returned unexpected PreviousLastAcked.", initialEventNumber, ack1);

        // Ack Event 3 before Event 2. The previous last ack must be 1 (since that's our last ack).
        val ack3 = ws.appendSuccessful(event3);
        Assert.assertEquals("appendSuccessful(3) returned unexpected PreviousLastAcked.", event1, ack3);

        // Ack Event 2. The previous ack was 3, so return that.
        val ack2 = ws.appendSuccessful(event2);
        Assert.assertEquals("appendSuccessful(2) returned unexpected PreviousLastAcked.", event3, ack2);
        Assert.assertEquals(WriterState.NO_FAILED_EVENT_NUMBER, ws.getLowestFailedEventNumber());
        Assert.assertEquals(24L, ws.getEventSizeForAppend());
    }

    /**
     * Tests {@link WriterState#beginAppend} and {@link WriterState#conditionalAppendFailed}.
     */
    @Test
    public void testConditionalAppendFailed() {
        // Begin with recording 2 Events.
        final long initialEventNumber = 1;
        val ws = new WriterState(initialEventNumber);
        long event1 = initialEventNumber + 1;
        ws.beginAppend(event1);
        long event2 = event1 + 1;
        ws.beginAppend(event2);

        // Ack Event 1 and conditionally-fail event 2.
        ws.appendSuccessful(event1);
        ws.conditionalAppendFailed(event2);

        // Event 3 comes now. Since we conditionally failed the last Event, the Last Stored Event Number should be
        // restored to Event 1.
        long event3 = event2 + 1;
        val begin3 = ws.beginAppend(event3);
        Assert.assertEquals("beginAppend(3) returned unexpected LastStoredEventNumber.", event1, begin3);
        Assert.assertEquals(WriterState.NO_FAILED_EVENT_NUMBER, ws.getLowestFailedEventNumber());
    }

    /**
     * Tests {@link WriterState#appendFailed} and {@link WriterState#fetchEligibleDelayedErrorHandler}.
     * We test the foll0wing scenario:
     * 1. Begin and successfully complete Event E1.
     * 2. Begin 2 events (E2 and E3), and fail the second one (E3).
     * 3. Begin 2 events (E4 and E5), and fail the second one (E5).
     * 4. Begin 1 event E6, and fail it.
     * 5. Complete E2 with success and fail E4.
     * <p>
     * What we expect:
     * - E1 has no effect on the test.
     * - When we fail E3, no callbacks are expected.
     * - When we complete E2, expect the callback from E3 to be invoked (and no other callbacks).
     * - When we fail E4, we expect its callback to be invoked, as well as E5's and E6's callback.
     */
    @Test
    public void testAppendFailed() {
        final long initialEventNumber = 1;
        long currentEventNumber = initialEventNumber;
        val ws = new WriterState(initialEventNumber);
        val callbackOrders = new ArrayList<Long>();

        // Begin and complete an event E1.
        val event1 = ++currentEventNumber;
        ws.beginAppend(event1);
        ws.appendSuccessful(event1);

        // Begin 2 events: E2 and E3.
        val event2 = ++currentEventNumber;
        ws.beginAppend(event2);
        val event3 = ++currentEventNumber;
        ws.beginAppend(event3);

        Assert.assertNull("Not expecting a result yet from getDelayedErrorHandlerIfEligible().", ws.fetchEligibleDelayedErrorHandler());

        // Indicate that E3 failed.
        setupAppendFailed(ws, event3, callbackOrders);
        Assert.assertEquals("Unexpected value from getLowestFailedEventNumber", event3, ws.getLowestFailedEventNumber());
        checkDelayedErrorHandler(ws.fetchEligibleDelayedErrorHandler(), 0, 1);

        // Begin two more events: E4 and E5.
        val event4 = ++currentEventNumber;
        ws.beginAppend(event4);
        val event5 = ++currentEventNumber;
        ws.beginAppend(event5);

        // ... and then indicate that E5 failed (nothing yet on E4).
        setupAppendFailed(ws, event5, callbackOrders);
        Assert.assertEquals("Unexpected value from getLowestFailedEventNumber", event3, ws.getLowestFailedEventNumber());

        // Begin one more event (E6)...
        val event6 = ++currentEventNumber;
        ws.beginAppend(event6);

        // .. and then indicate it failed.
        setupAppendFailed(ws, event6, callbackOrders);
        Assert.assertEquals("Unexpected value from getLowestFailedEventNumber", event3, ws.getLowestFailedEventNumber());

        checkDelayedErrorHandler(ws.fetchEligibleDelayedErrorHandler(), 0, 3);

        // Complete E2 successfully. E3 is already failed, so we expect its callback to be invoked now
        ws.appendSuccessful(event2);
        val deh1 = ws.fetchEligibleDelayedErrorHandler();
        checkDelayedErrorHandler(deh1, 1, 2);
        deh1.getHandlersToExecute().forEach(Runnable::run); // Run the callbacks to validate that we aren't invoking them multiple times.

        // Fail E4. E5 and E6 are already failed, so we expect both their callbacks to be invoked now.
        setupAppendFailed(ws, event4, callbackOrders);
        Assert.assertEquals("Unexpected value from getLowestFailedEventNumber", event3, ws.getLowestFailedEventNumber());
        val deh2 = ws.fetchEligibleDelayedErrorHandler();
        checkDelayedErrorHandler(deh2, 3, 0);
        deh2.getHandlersToExecute().forEach(Runnable::run); // Run the callbacks to validate that we aren't invoking them multiple times.

        checkDelayedErrorHandler(ws.fetchEligibleDelayedErrorHandler(), 0, 0);

        AssertExtensions.assertListEquals("Callbacks not invoked in correct order.",
                Arrays.asList(event3, event4, event5, event6), callbackOrders, Long::equals);
    }

    private void setupAppendFailed(WriterState ws, long eventNumber, ArrayList<Long> callbackOrders) {
        ws.appendFailed(eventNumber, () -> callbackOrders.add(eventNumber));
    }

    private void checkDelayedErrorHandler(WriterState.DelayedErrorHandler deh, int expectedToRun, int expectedHandlersRemaining) {
        Assert.assertNotNull("Expecting a result from getDelayedErrorHandlerIfEligible()", deh);
        Assert.assertEquals("Unexpected number of callbacks returned.", expectedToRun, deh.getHandlersToExecute().size());
        Assert.assertEquals("Unexpected number of handlers remaining.", expectedHandlersRemaining, deh.getHandlersRemaining());
    }
}
