package io.pravega.segmentstore.server.writer;

import org.junit.Assert;
import org.junit.Test;

/**
 * Writer Config Test
 */
public class WriterConfigTest {

    @Test
    public void testMaxRolloverSize() {
        Assert.assertEquals(134217728L, (long)WriterConfig.MAX_ROLLOVER_SIZE.getDefaultValue());
        Assert.assertEquals("rollover.size.bytes.max", WriterConfig.MAX_ROLLOVER_SIZE.getName());
    }
}
