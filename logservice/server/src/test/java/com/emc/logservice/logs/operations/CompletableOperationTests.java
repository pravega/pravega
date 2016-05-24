package com.emc.logservice.logs.operations;

import com.emc.logservice.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for CompletableOperation class.
 */
public class CompletableOperationTests {
    private static final long DefaultSeqNo = -1;
    private static final long ValidSeqNo = 1;

    /**
     * Tests the functionality of the complete() method.
     */
    @Test
    public void testComplete() {
        MetadataPersistedOperation op = new MetadataPersistedOperation();

        AtomicLong callbackSeqNo = new AtomicLong(DefaultSeqNo);
        AtomicBoolean failureCallbackCalled = new AtomicBoolean();
        CompletableOperation co = new CompletableOperation(op, callbackSeqNo::set, ex -> failureCallbackCalled.set(true));

        AssertExtensions.assertThrows("complete() succeeded even if Operation had no Sequence Number.",
                co::complete,
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("Success callback was invoked for illegal complete() call.", DefaultSeqNo, callbackSeqNo.get());
        Assert.assertFalse("Failure callback was invoked for illegal complete() call.", failureCallbackCalled.get());

        op.setSequenceNumber(ValidSeqNo);
        co.complete();
        Assert.assertEquals("Success callback not invoked with the correct argument after valid complete() call.", ValidSeqNo, callbackSeqNo.get());
        Assert.assertFalse("Failure callback was invoked for valid complete() call.", failureCallbackCalled.get());
    }

    /**
     * Tests the functionality of the fail() method.
     */
    @Test
    public void testFail() {
        MetadataPersistedOperation op = new MetadataPersistedOperation();

        AtomicBoolean successCallbackCalled = new AtomicBoolean();
        AtomicBoolean failureCallbackCalled = new AtomicBoolean();
        CompletableOperation co = new CompletableOperation(op, seqNo -> successCallbackCalled.set(true), ex -> failureCallbackCalled.set(true));

        co.fail(new Exception("Intentional"));
        Assert.assertTrue("Failure callback was not invoked for valid fail() call.", failureCallbackCalled.get());
        Assert.assertFalse("Success callback invoked for valid fail() call.", successCallbackCalled.get());
    }
}
