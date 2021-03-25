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
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

public class ReusableLatchTests {

    @Test(timeout = 5000)
    public void testRelease() {
        ReusableLatch latch = new ReusableLatch(false);
        Assert.assertEquals(0, latch.getQueueLength());
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
