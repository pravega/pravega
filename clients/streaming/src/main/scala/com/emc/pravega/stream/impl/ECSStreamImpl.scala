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

import java.util.Collections

import com.emc.pravega.stream.{Consumer, ConsumerConfig, EventRouter, Position, Producer, ProducerConfig, RateChangeListener, SegmentId, Serializer, Stream, StreamConfiguration, StreamSegments}

/**
  * Created by kandha on 7/26/16.
  */

class ECSSingleSegmentStreamImpl(ipList: String, accessKey: String, secretKey: String,
                                 val scope: String, val name: String, val config: StreamConfiguration) extends Stream {

  this.logId = new SegmentId(scope, name, 1, 0)

  final private var logId: SegmentId = _

  /**
    * No implementation as right now we are just testing ECS perf
    */
  final private val router: EventRouter = new EventRouter() {
    def getSegmentForEvent(stream: Stream, routingKey: String): SegmentId = logId
  }

  /**
    * No implementation as right now we are just testing ECS perf
    * @param time
    * @return
    */
  def getSegments(time: Long): StreamSegments = new StreamSegments(Collections.singletonList(logId), time)

  /**
    * No implementation as right now we are just testing ECS perf
    * @return
    */
  def getLatestSegments: StreamSegments = getSegments(System.currentTimeMillis)

  /**
    * No implementation as right now we are just testing ECS perf
    * @param time
    * @return
    */
  def getRate(time: Long): Long = 0

  /**
    * Creates a producer which writes to just one object per stream.
    * TODO: This can be improved to write to different objects for different segments once we find perf manageable
    *
    * @param s
    * @param config
    * @tparam T
    * @return
    */
  def createProducer[T](s: Serializer[T], config: ProducerConfig): Producer[T] = new ECSProducerImpl[T](ipList, accessKey, secretKey, this, router, s, config)

  /**
    * No implementation as right now we are just testing ECS perf
    * @param s
    * @param config
    * @param startingPosition
    * @param l
    * @tparam T
    * @return
    */
  def createConsumer[T](s: Serializer[T], config: ConsumerConfig, startingPosition: Position, l: RateChangeListener): Consumer[T] = {
    null
    // return new ConsumerImpl<>(this, logClient, s, startingPosition,
    // orderer, l, config);
  }

  override def getConfig: StreamConfiguration = this.config

  override def getName: String = this.name
}
