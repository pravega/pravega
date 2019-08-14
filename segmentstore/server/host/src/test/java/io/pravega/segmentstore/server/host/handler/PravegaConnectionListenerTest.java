/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class PravegaConnectionListenerTest {

    @Test
    public void testCtorSetsTlsReloadFalseByDefault() {
        PravegaConnectionListener listener = new PravegaConnectionListener(false, 6222,
                mock(StreamSegmentStore.class), mock(TableStore.class));
        assertFalse(listener.isEnableTlsReload());
    }

    @Test
    public void testCtorSetsTlsReloadFalseIfTlsIsDisabled() {
        PravegaConnectionListener listener = new PravegaConnectionListener(false, true,
                "localhost", 6222, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                null, null, true);
        assertFalse(listener.isEnableTlsReload());
    }

    @Test
    public void testCloseWithoutStartListeningThrowsNoException() {
        PravegaConnectionListener listener = new PravegaConnectionListener(true, true,
                "localhost", 6222, mock(StreamSegmentStore.class), mock(TableStore.class),
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                null, null, true);

        // Note that we do not invoke startListening() here, which among other things instantiates some of the object
        // state that is cleaned up upon invocation of close() in this line.
        listener.close();
    }
}
