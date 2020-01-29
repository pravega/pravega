/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

public class ReusableLatchTests {

    @Test(timeout = 5000)
    public void testRelease() {
        ReusableLatch latch = new ReusableLatch(false);
        AssertExtensions.assertBlocks(() -> latch.awaitUninterruptibly(), () -> latch.release());
    }
    
    @Test(timeout = 5000)
    public void testAlreadyRelease() throws InterruptedException {
        ReusableLatch latch = new ReusableLatch(false);
        latch.release();
        latch.await();

        latch = new ReusableLatch(true);
        latch.await(); 
    }
    
}
