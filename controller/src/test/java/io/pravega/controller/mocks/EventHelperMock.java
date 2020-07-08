/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.task.EventHelper;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class EventHelperMock {

    public static EventHelper getEventHelperMock(ScheduledExecutorService executor, String hostId, HostIndex hostIndex) {
        return new EventHelper(executor, hostId, hostIndex);
    }

    public static EventHelper getFailingEventHelperMock() {
        EventHelper mockHelper = mock(EventHelper.class);
        doReturn(Futures.failedFuture(new CompletionException(new RuntimeException()))).when(mockHelper)
                .addIndexAndSubmitTask(any(), any());
        return mockHelper;
    }
}
