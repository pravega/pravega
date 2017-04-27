package io.pravega.controller.eventProcessor;

import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class EventTest {
    @Test
    public void testEventKey() {
        UUID txid = UUID.randomUUID();
        String scope = "test";
        String stream = "test";
        AbortEvent abortEvent = new AbortEvent(scope, stream, txid);
        CommitEvent commitEvent = new CommitEvent(scope, stream, txid);
        assertEquals(abortEvent.getKey(), "test/test");
        assertEquals(commitEvent.getKey(), "test/test");
    }
}
