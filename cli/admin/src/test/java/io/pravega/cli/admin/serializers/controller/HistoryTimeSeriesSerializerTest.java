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
package io.pravega.cli.admin.serializers.controller;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static io.pravega.cli.admin.serializers.AbstractSerializer.appendField;
import static io.pravega.cli.admin.serializers.AbstractSerializer.appendFieldWithCustomDelimiters;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToString;
import static io.pravega.cli.admin.serializers.AbstractSerializer.convertCollectionToStringWithCustomDelimiter;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_HISTORY_RECORDS;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_EPOCH;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_LIST_ENTRY_DELIMITER;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_SCALE_TIME;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED;
import static io.pravega.cli.admin.serializers.controller.HistoryTimeSeriesSerializer.HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER;
import static io.pravega.cli.admin.utils.TestUtils.generateStreamSegmentRecordString;
import static org.junit.Assert.assertEquals;

public class HistoryTimeSeriesSerializerTest {

    @Test
    public void testHistoryTimeSeriesSerializer() {
        StringBuilder userGeneratedMetadataBuilder = new StringBuilder();
        appendField(userGeneratedMetadataBuilder, HISTORY_TIME_SERIES_HISTORY_RECORDS,
                convertCollectionToStringWithCustomDelimiter(
                        ImmutableList.of(
                                generateHistoryTimeSeriesRecordString(1, 1, 10L,
                                        ImmutableList.of(generateStreamSegmentRecordString(1, 2, 2L, 0.1, 0.2)),
                                        ImmutableList.of(generateStreamSegmentRecordString(2, 3, 3L, 0.3, 0.5))),
                                generateHistoryTimeSeriesRecordString(2, 2, 100L,
                                        ImmutableList.of(generateStreamSegmentRecordString(4, 5, 4L, 1.2, 1.3)),
                                        ImmutableList.of(generateStreamSegmentRecordString(5, 6, 5L, 2.3, 3.3)))),
                        s -> s, HISTORY_TIME_SERIES_RECORD_LIST_ENTRY_DELIMITER));

        String userString = userGeneratedMetadataBuilder.toString();
        HistoryTimeSeriesSerializer serializer = new HistoryTimeSeriesSerializer();
        ByteBuffer buf = serializer.serialize(userString);
        assertEquals(userString, serializer.deserialize(buf));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHistoryTimeSeriesSerializerArgumentFailure() {
        String userString = "";
        HistoryTimeSeriesSerializer serializer = new HistoryTimeSeriesSerializer();
        serializer.serialize(userString);
    }

    private String generateHistoryTimeSeriesRecordString(int epoch, int referenceEpoch, long scaleTime,
                                                         List<String> segmentsSealed, List<String> segmentsCreated) {
        StringBuilder builder = new StringBuilder();
        appendFieldWithCustomDelimiters(builder, HISTORY_TIME_SERIES_RECORD_EPOCH, String.valueOf(epoch),
                HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(builder, HISTORY_TIME_SERIES_RECORD_REFERENCE_EPOCH, String.valueOf(referenceEpoch),
                HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(builder, HISTORY_TIME_SERIES_RECORD_SCALE_TIME, String.valueOf(scaleTime),
                HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(builder, HISTORY_TIME_SERIES_RECORD_SEGMENTS_SEALED, convertCollectionToString(segmentsSealed, s -> s),
                HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        appendFieldWithCustomDelimiters(builder, HISTORY_TIME_SERIES_RECORD_SEGMENTS_CREATED, convertCollectionToString(segmentsCreated, s -> s),
                HISTORY_TIME_SERIES_RECORD_PAIR_DELIMITER, HISTORY_TIME_SERIES_RECORD_VALUE_DELIMITER);
        return builder.toString();
    }
}
