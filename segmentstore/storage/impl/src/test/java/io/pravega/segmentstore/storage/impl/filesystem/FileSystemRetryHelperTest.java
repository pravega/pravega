/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class FileSystemRetryHelperTest {
    //Region RetryHelper tests
    @Test
    public void retry() throws Exception {
        // Retry fails if the condition is always met
        final boolean testValue = false;
        assertThrows( "Retry should throw when the condition does not change after retries",
                () -> FileSystemRetryHelper.retry(() -> testValue,
                        (bool) -> !bool,
                        () -> new Exception("Still false"),
                        3),
                (ex) -> ex instanceof Exception);

        // Retry passes in first attempt
        FileSystemRetryHelper.retry(() -> !testValue,
                (bool) -> !bool,
                () -> new Exception("Still false"),
                3);

        // Retry passes in third attempt
        AtomicInteger check = new AtomicInteger(0);

        FileSystemRetryHelper.retry(() -> check.incrementAndGet() == 3,
                (bool) -> !bool,
                () -> new Exception("Still false"),
                3);
    }
    //endregion
}
