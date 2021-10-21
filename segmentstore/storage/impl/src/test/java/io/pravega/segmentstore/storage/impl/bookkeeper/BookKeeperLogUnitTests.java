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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.WriteFailureException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.BKException;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for BookKeeperLog. These tests do not need to start a BookKeeper
 * cluster.
 */
public class BookKeeperLogUnitTests {

    @Test
    public void testGenericRuntimeException() {
        testHandleWriteException(new RuntimeException(), DurableDataLogException.class);
    }

    @Test
    public void testBKLedgerFencedException() {
        testHandleWriteException(new BKException.BKLedgerFencedException(), DataLogWriterNotPrimaryException.class);
    }

    @Test
    public void testBKNotEnoughBookiesException() {
        testHandleWriteException(new BKException.BKNotEnoughBookiesException(), DataLogNotAvailableException.class);
    }

    @Test
    public void testBKLedgerClosedException() {
        testHandleWriteException(new BKException.BKLedgerClosedException(), WriteFailureException.class);
    }

    @Test
    public void testBKWriteException() {
        testHandleWriteException(new BKException.BKWriteException(), WriteFailureException.class);
    }

    @Test
    public void testBKClientClosedException() {
        testHandleWriteException(new BKException.BKClientClosedException(), ObjectClosedException.class);
    }

    @Test
    public void testBKUnexpectedConditionException() {
        testHandleWriteException(new BKException.BKUnexpectedConditionException(), DurableDataLogException.class);
    }

    private static Write testHandleWriteException(Throwable ex, Class expectedErrorType) {
        Write result = new Write(new CompositeByteArraySegment(new byte[0]), mock(WriteLedger.class), new CompletableFuture<>());
        BookKeeperLog bookKeeperLog = mock(BookKeeperLog.class);
        BookKeeperLog.handleWriteException(ex, result, bookKeeperLog);
        assertTrue("Unexpected failure cause " + result.getFailureCause(), expectedErrorType.isInstance(result.getFailureCause()));
        assertSame(ex, result.getFailureCause().getCause());
        return result;
    }
}
