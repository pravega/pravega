/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.service.server.stats;

public interface SegmentStatsRecorder {

    void createSegment(String streamSegmentName, byte type, long targetRate);

    void sealSegment(String streamSegmentName);

    void policyUpdate(String segmentStreamName, byte type, long targetRate);

    void record(String streamSegmentName, long dataLength, int numOfEvents);

    void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime);

    //    static long getTargetRate(Map<UUID, Long> attributes) {
    //        return attributes.getOrDefault(TARGET_RATE, 0L);
    //    }
    //
    //    static byte getScaleType(Map<UUID, Long> attributes) {
    //        Long scaleType = attributes.getOrDefault(SCALE_TYPE, 0L);
    //        return scaleType.byteValue();
    //    }
    //
    //    static void putScaleType(Map<UUID, Long> attributes, byte scaleType) {
    //        attributes.put(SCALE_TYPE, Byte.toUnsignedLong(scaleType));
    //    }
    //
    //    static void putTargetRate(Map<UUID, Long> attributes, int targetRate) {
    //        attributes.put(TARGET_RATE, Integer.toUnsignedLong(targetRate));
    //    }
}
