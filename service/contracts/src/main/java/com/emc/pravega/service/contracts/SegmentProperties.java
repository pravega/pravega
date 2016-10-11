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

package com.emc.pravega.service.contracts;

import java.util.Date;

/**
 * General properties about a StreamSegment.
 */
public interface SegmentProperties {
    /**
     * Gets a value indicating the name of this StreamSegment.
     */
    String getName();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     */
    boolean isDeleted();

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     */
    Date getLastModified();

    /**
     * Gets a value indicating the full, readable length of the StreamSegment.
     */
    long getLength();


}

