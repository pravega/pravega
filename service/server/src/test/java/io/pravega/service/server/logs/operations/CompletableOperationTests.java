/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.testcommon.IntentionalException;
import org.junit.Assert;
import org.junit.Test;

import io.pravega.testcommon.AssertExtensions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for CompletableOperation class.
 */
public class CompletableOperationTests {
    private static final long DEFAULT_SEQ_NO = Operation.NO_SEQUENCE_NUMBER;
    private static final long VALID_SEQ_NO = 1;

    /**
     * Tests the functionality of the complete() method.
     */
    @Test
    public void testComplete() {
        MetadataCheckpointOperation op = new MetadataCheckpointOperation();

        AtomicLong callbackSeqNo = new AtomicLong(DEFAULT_SEQ_NO);
        AtomicBoolean failureCallbackCalled = new AtomicBoolean();
        CompletableOperation co = new CompletableOperation(op, callbackSeqNo::set, ex -> failureCallbackCalled.set(true));

        AssertExtensions.assertThrows("complete() succeeded even if Operation had no Sequence Number.",
                co::complete,
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("Success callback was invoked for illegal complete() call.", DEFAULT_SEQ_NO, callbackSeqNo.get());
        Assert.assertFalse("Failure callback was invoked for illegal complete() call.", failureCallbackCalled.get());

        op.setSequenceNumber(VALID_SEQ_NO);
        co.complete();
        Assert.assertEquals("Success callback not invoked with the correct argument after valid complete() call.", VALID_SEQ_NO, callbackSeqNo.get());
        Assert.assertFalse("Failure callback was invoked for valid complete() call.", failureCallbackCalled.get());
    }

    /**
     * Tests the functionality of the fail() method.
     */
    @Test
    public void testFail() {
        MetadataCheckpointOperation op = new MetadataCheckpointOperation();

        AtomicBoolean successCallbackCalled = new AtomicBoolean();
        AtomicBoolean failureCallbackCalled = new AtomicBoolean();
        CompletableOperation co = new CompletableOperation(op, seqNo -> successCallbackCalled.set(true), ex -> failureCallbackCalled.set(true));

        co.fail(new IntentionalException());
        Assert.assertTrue("Failure callback was not invoked for valid fail() call.", failureCallbackCalled.get());
        Assert.assertFalse("Success callback invoked for valid fail() call.", successCallbackCalled.get());
    }
}
