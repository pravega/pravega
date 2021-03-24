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

public interface SegmentStatsRecorder extends AutoCloseable {

    /**
     * Method to notify segment create events.
     *
     * @param streamSegmentName segment.
     * @param type              type of auto scale.
     * @param targetRate        desired rate.
     * @param elapsed           The amount of time elapsed for the creation of this segment.
     */
    void createSegment(String streamSegmentName, byte type, int targetRate, Duration elapsed);

    /**
     * Method to notify a segment was deleted.
     *
     * @param segmentName The segment name.
     */
    void deleteSegment(String segmentName);

    /**
     * Method to notify segment sealed events.
     *
     * @param streamSegmentName segment.
     */
    void sealSegment(String streamSegmentName);

    /**
     * Method to notify segment policy events.
     *
     * @param streamSegmentName segment.
     * @param type              type of auto scale.
     * @param targetRate        desired rate.
     */
    void policyUpdate(String streamSegmentName, byte type, int targetRate);

    /**
     * Method to record incoming traffic.
     *
     * @param streamSegmentName segment name.
     * @param dataLength        data length.
     * @param numOfEvents       number of events.
     * @param elapsed           The amount of time elapsed for the append to process.
     */
    void recordAppend(String streamSegmentName, long dataLength, int numOfEvents, Duration elapsed);

    /**
     * Method to notify merge of transaction.
     *
     * @param streamSegmentName target segment.
     * @param dataLength        data in transactional segment.
     * @param numOfEvents       events in transactional segment.
     * @param txnCreationTime   transaction creation time.
     */
    void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime);

    /**
     * Method to notify a read operation completed.
     *
     * @param elapsed The amount of time elapsed for the read to complete.
     */
    void readComplete(Duration elapsed);

    /**
     * Method to notify a chunk of data has been read.
     *
     * @param segment The name of the segment.
     * @param length  The length of the data read.
     */
    void read(String segment, int length);

    @Override
    void close();

    //region NoOp

    /**
     * Creates a new {@link SegmentStatsRecorder} instance that does not do anything.
     */
    static SegmentStatsRecorder noOp() {
        return new SegmentStatsRecorder() {
            @Override
            public void createSegment(String streamSegmentName, byte type, int targetRate, Duration elapsed) {
            }

            @Override
            public void deleteSegment(String segmentName) {
            }

            @Override
            public void sealSegment(String streamSegmentName) {
            }

            @Override
            public void policyUpdate(String streamSegmentName, byte type, int targetRate) {
            }

            @Override
            public void recordAppend(String streamSegmentName, long dataLength, int numOfEvents, Duration elapsed) {
            }

            @Override
            public void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
            }

            @Override
            public void readComplete(Duration elapsed) {
            }

            @Override
            public void read(String segment, int length) {
            }

            @Override
            public void close() {
            }
        };
    }

    //endregion
}
