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
package com.emc.pravega.stream;

import java.io.Serializable;

/**
 * The configuration of a Stream.
 */
public interface StreamConfiguration extends Serializable {
    
    /**
     * Api to return scope.
     * @return The scope of the stream.
     */
    String getScope();

    /**
     * Api to return stream name.
     * @return The name of the stream.
     */
    String getName();

    /**
     * Api to return scaling policy.
     * @return The stream's scaling policy.
     */
    ScalingPolicy getScalingPolicy();
    
    /**
     * Returns the retention policy for this stream.
     */
    RetentionPolicy getRetentionPolicy();
}
