/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common;

public interface SegmentStoreMetricsNames {
    // Stream Segment request Operations
    public final static String CREATE_SEGMENT = "CREATE_SEGMENT";
    public final static String DELETE_SEGMENT = "DELETE_SEGMENT";
    public final static String READ_SEGMENT = "READ_SEGMENT";
    // Bytes read by READ_SEGMENT operation, for read throughput
    public final static String SEGMENT_READ_BYTES = "SEGMENT_READ_BYTES";
    // Counter for all read bytes.
    public final static String ALL_READ_BYTES = "ALL_READ_BYTES";
    // Gauge for pending append bytes
    public final static String PENDING_APPEND_BYTES = "PENDING_APPEND_BYTES";

    //HDFS stats
    public final static String HDFS_READ_LATENCY = "HDFSReadLatencyMillis";
    public static final String HDFS_WRITE_LATENCY = "HDFSWriteLatencyMillis";
    public static final String HDFS_READ_BYTES = "HDFSReadBytes";
    public static final String HDFS_WRITTEN_BYTES = "HDFSWriteBytes";

}
