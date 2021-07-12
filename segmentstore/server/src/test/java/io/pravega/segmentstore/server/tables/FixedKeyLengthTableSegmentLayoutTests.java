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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for {@link ContainerTableExtensionImpl} when using a {@link FixedKeyLengthTableSegmentLayout} for its
 * Table Segments.
 */
public class FixedKeyLengthTableSegmentLayoutTests extends TableSegmentLayoutTestBase {
    private static final TableSegmentConfig DEFAULT_CONFIG = TableSegmentConfig.builder().keyLength(64).build();
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    //region TableSegmentLayoutTestBase Implementation

    @Override
    protected int getKeyLength() {
        return DEFAULT_CONFIG.getKeyLength();
    }

    @Override
    protected void createSegment(TableContext context, String segmentName) {
        context.ext.createSegment(SEGMENT_NAME, SegmentType.builder().fixedKeyLengthTableSegment().build(), DEFAULT_CONFIG, TIMEOUT).join();
    }

    @Override
    protected Map<AttributeId, Long> getExpectedNewSegmentAttributes(TableContext context) {
        val result = new HashMap<AttributeId, Long>();
        result.put(Attributes.ATTRIBUTE_ID_LENGTH, (long) DEFAULT_CONFIG.getKeyLength());
        result.putAll(context.ext.getConfig().getDefaultCompactionAttributes());
        return result;
    }

    @Override
    protected boolean supportsDeleteIfEmpty() {
        return false;
    }

    @Override
    protected WriterTableProcessor createWriterTableProcessor(ContainerTableExtension ext, TableContext context) {
        return null;
    }

    @Override
    protected boolean shouldExpectWriterTableProcessors() {
        return false;
    }

    @Override
    protected IteratorArgs createEmptyIteratorArgs() {
        return IteratorArgs
                .builder()
                .fetchTimeout(TIMEOUT)
                .from(new ByteArraySegment(BufferViewComparator.getMaxValue(DEFAULT_CONFIG.getKeyLength())))
                .build();
    }

    @Override
    protected void checkTableAttributes(int totalUpdateCount, int totalRemoveCount, int uniqueKeyCount, TableContext context) {
        val attributes = context.segment().getInfo().getAttributes();
        Assert.assertEquals(totalUpdateCount, (long) attributes.get(TableAttributes.TOTAL_ENTRY_COUNT));
        val expectedUniqueKeyCount = totalUpdateCount - totalRemoveCount;
        val actualUniqueKeyCount = context.segment().getExtendedAttributeCount(TIMEOUT).join();
        Assert.assertEquals(expectedUniqueKeyCount, (long) actualUniqueKeyCount);
    }

    //endregion

    @Override
    @Test
    public void testDeleteIfEmpty() {
        @Cleanup
        val context = new TableContext(executorService());
        createSegment(context, SEGMENT_NAME);
        AssertExtensions.assertSuppliedFutureThrows("Not expecting mustBeEmpty to be supported.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
    }
}
