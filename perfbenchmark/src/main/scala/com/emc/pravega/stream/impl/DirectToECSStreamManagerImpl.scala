/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *   <p>
 *    http://www.apache.org/licenses/LICENSE-2.0
 *   <p>
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.emc.pravega.stream.impl

import com.emc.pravega.stream.{Stream, StreamConfiguration, StreamManager}

class DirectToECSStreamManagerImpl(ipList: String, accessKey:String,secretKey:String,
                                   scope: String)  extends StreamManager {

  /**
    * Create a stream that writes directly to ECS
    * @param streamName
    * @param config
    * @return
    */
    @Override
    def createStream(streamName: String , config: StreamConfiguration ) : Stream = {
        return new ECSSingleSegmentStreamImpl(ipList, accessKey, secretKey,scope, streamName,config)
    }

  /**
    * No implementation as right now we are just testing ECS perf
    * @param streamName
    * @param config
    */
    @Override
    def alterStream(streamName: String , config: StreamConfiguration): Unit = {

    }

  /**
    * No implementation as right now we are just testing ECS perf
    * @param streamName
    * @return
    */
    @Override
    def getStream(streamName: String) : Stream = null

  /**
    * No implementation as right now we are just testing ECS perf
    */
    @Override
    def close() : Unit = {

    }
}

