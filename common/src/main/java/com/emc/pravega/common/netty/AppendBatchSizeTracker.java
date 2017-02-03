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
package com.emc.pravega.common.netty;

public interface AppendBatchSizeTracker {

    /**
     * Records that an append has been sent.
     * 
     * @param eventNumber the number of the event
     * @param size the size of the event
     */
    void recordAppend(long eventNumber, int size);

    /**
     * Records that one or more events have been acked.
     * 
     * @param eventNumber the nubmer of the last event
     */
    void recordAck(long eventNumber);

    /**
     * Returns the size that should be used for the next append block.
     */
    int getAppendBlockSize();
    
    /**
     * Returns the timeout that should be used for append blocks.
     */
    int getBatchTimeout();

}