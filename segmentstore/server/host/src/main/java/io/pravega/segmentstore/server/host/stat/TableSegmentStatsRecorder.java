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
package io.pravega.segmentstore.server.host.stat;

import java.time.Duration;

/**
 * Defines a Stat Recorder for Table Segments.
 */
public interface TableSegmentStatsRecorder extends AutoCloseable {
    /**
     * Notifies a Table Segment has been created.
     *
     * @param tableSegmentName Table Segment Name.
     * @param elapsed          Elapsed time.
     */
    void createTableSegment(String tableSegmentName, Duration elapsed);

    /**
     * Notifies a Table Segment has been deleted.
     *
     * @param tableSegmentName Table Segment Name.
     * @param elapsed          Elapsed time.
     */
    void deleteTableSegment(String tableSegmentName, Duration elapsed);

    /**
     * Notifies a set of Entries has been updated.
     *
     * @param tableSegmentName Table Segment Name.
     * @param entryCount       Number of entries updated.
     * @param conditional      Whether the update is conditional.
     * @param elapsed          Elapsed time.
     */
    void updateEntries(String tableSegmentName, int entryCount, boolean conditional, Duration elapsed);

    /**
     * Notifies a set of Keys has been removed.
     *
     * @param tableSegmentName Table Segment Name.
     * @param keyCount         Number of keys removed.
     * @param conditional      Whether the removal is conditional.
     * @param elapsed          Elapsed time.
     */
    void removeKeys(String tableSegmentName, int keyCount, boolean conditional, Duration elapsed);

    /**
     * Notifies a set of Keys have been retrieved.
     *
     * @param tableSegmentName Table Segment Name.
     * @param keyCount         Number of keys retrieved.
     * @param elapsed          Elapsed time.
     */
    void getKeys(String tableSegmentName, int keyCount, Duration elapsed);

    /**
     * Notifies a Table Key iteration completed.
     *
     * @param tableSegmentName Table Segment Name.
     * @param resultCount      Number of Keys in the iteration result.
     * @param elapsed          Elapsed time.
     */
    void iterateKeys(String tableSegmentName, int resultCount, Duration elapsed);

    /**
     * Notifies a Table Entry iteration completed.
     *
     * @param tableSegmentName Table Segment Name.
     * @param resultCount      Number of Entries in the iteration result.
     * @param elapsed          Elapsed time.
     */
    void iterateEntries(String tableSegmentName, int resultCount, Duration elapsed);

    /**
     * Notifies that a Get Table Segment Info was invoked.
     *
     * @param tableSegmentName Table Segment Name.
     * @param elapsed          Elapsed time.
     */
    void getInfo(String tableSegmentName, Duration elapsed);

    @Override
    void close();

    //region NoOp

    /**
     * Creates a new {@link TableSegmentStatsRecorder} instance that does not do anything.
     */
    static TableSegmentStatsRecorder noOp() {
        return new TableSegmentStatsRecorder() {
            @Override
            public void createTableSegment(String tableSegmentName, Duration elapsed) {
            }

            @Override
            public void deleteTableSegment(String tableSegmentName, Duration elapsed) {
            }

            @Override
            public void updateEntries(String tableSegmentName, int entryCount, boolean conditional, Duration elapsed) {
            }

            @Override
            public void removeKeys(String tableSegmentName, int keyCount, boolean conditional, Duration elapsed) {
            }

            @Override
            public void getKeys(String tableSegmentName, int keyCount, Duration elapsed) {
            }

            @Override
            public void iterateKeys(String tableSegmentName, int resultCount, Duration elapsed) {
            }

            @Override
            public void iterateEntries(String tableSegmentName, int resultCount, Duration elapsed) {
            }

            @Override
            public void getInfo(String tableSegmentName, Duration elapsed) {

            }

            @Override
            public void close() {
            }
        };
    }

    //endregion
}
