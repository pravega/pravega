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
package io.pravega.segmentstore.server.writer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Writer Config Test
 */
public class WriterConfigTest {

    @Test
    public void testDefaultValues() {
        Assert.assertEquals(134217728L, (long) WriterConfig.MAX_ROLLOVER_SIZE.getDefaultValue());
        Assert.assertEquals("rollover.size.bytes.max", WriterConfig.MAX_ROLLOVER_SIZE.getName());

        Assert.assertEquals(4 * 1024 * 1024, (int) WriterConfig.FLUSH_THRESHOLD_BYTES.getDefaultValue());
        Assert.assertEquals("flush.threshold.bytes", WriterConfig.FLUSH_THRESHOLD_BYTES.getName());

        Assert.assertEquals(30 * 1000L, (long) WriterConfig.FLUSH_THRESHOLD_MILLIS.getDefaultValue());
        Assert.assertEquals("flush.threshold.milliseconds", WriterConfig.FLUSH_THRESHOLD_MILLIS.getName());

        Assert.assertEquals(200, (int) WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD.getDefaultValue());
        Assert.assertEquals("flush.attributes.threshold", WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD.getName());

        Assert.assertEquals(WriterConfig.FLUSH_THRESHOLD_BYTES.getDefaultValue(), WriterConfig.MAX_FLUSH_SIZE_BYTES.getDefaultValue());
        Assert.assertEquals("flush.size.bytes.max", WriterConfig.MAX_FLUSH_SIZE_BYTES.getName());

        Assert.assertEquals(1000, (int) WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE.getDefaultValue());
        Assert.assertEquals("itemsToReadAtOnce.max", WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE.getName());

        Assert.assertEquals(2 * 1000L, (long) WriterConfig.MIN_READ_TIMEOUT_MILLIS.getDefaultValue());
        Assert.assertEquals("read.timeout.milliseconds.min", WriterConfig.MIN_READ_TIMEOUT_MILLIS.getName());

        Assert.assertEquals(30 * 60 * 1000L, (long) WriterConfig.MAX_READ_TIMEOUT_MILLIS.getDefaultValue());
        Assert.assertEquals("read.timeout.milliseconds.max", WriterConfig.MAX_READ_TIMEOUT_MILLIS.getName());

        Assert.assertEquals(1000L, (long) WriterConfig.ERROR_SLEEP_MILLIS.getDefaultValue());
        Assert.assertEquals("error.sleep.milliseconds", WriterConfig.ERROR_SLEEP_MILLIS.getName());

        Assert.assertEquals(60 * 1000L, (long) WriterConfig.FLUSH_TIMEOUT_MILLIS.getDefaultValue());
        Assert.assertEquals("flush.timeout.milliseconds", WriterConfig.FLUSH_TIMEOUT_MILLIS.getName());

        Assert.assertEquals(15 * 1000L, (long) WriterConfig.ACK_TIMEOUT_MILLIS.getDefaultValue());
        Assert.assertEquals("ack.timeout.milliseconds", WriterConfig.ACK_TIMEOUT_MILLIS.getName());

        Assert.assertEquals(10 * 1000L, (long) WriterConfig.SHUTDOWN_TIMEOUT_MILLIS.getDefaultValue());
        Assert.assertEquals("shutDown.timeout.milliseconds", WriterConfig.SHUTDOWN_TIMEOUT_MILLIS.getName());
    }
}
