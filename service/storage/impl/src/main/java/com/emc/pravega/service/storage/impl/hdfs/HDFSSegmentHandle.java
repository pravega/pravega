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

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.storage.SegmentHandle;
import org.apache.hadoop.fs.Path;

/**
 * SegmentHandle to be used by HDFSStorage.
 */
class HDFSSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final Path physicalSegmentPath;

    /**
     * Creates a new instance of the HDFSSegmentHandle class.
     *
     * @param segmentName         The name of the Segment, as it is visible inside Pravega and through its APIs.
     * @param physicalSegmentName The (current) name of the Segment, in HDFS Storage.
     */
    HDFSSegmentHandle(String segmentName, String physicalSegmentName) {
        this.segmentName = segmentName;
        this.physicalSegmentPath = new Path(physicalSegmentName);
    }

    @Override
    public String getSegmentName() {
        return this.segmentName;
    }

    /**
     * Gets a Path that represents the location of the segment in the HDFS instance.
     */
    Path getPhysicalSegmentPath() {
        return this.physicalSegmentPath;
    }

    @Override
    public String toString() {
        return String.format("Name = %s, Path = %s", this.segmentName, this.physicalSegmentPath);
    }
}