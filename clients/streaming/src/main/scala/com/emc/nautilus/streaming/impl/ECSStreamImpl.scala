package com.emc.nautilus.streaming.impl

import java.util.Collections

import com.emc.nautilus.streaming.{Consumer, ConsumerConfig, EventRouter, Position, Producer, ProducerConfig, RateChangeListener, SegmentId, Serializer, Stream, StreamConfiguration, StreamSegments}

/**
  * Created by kandha on 7/26/16.
  */

class ECSSingleSegmentStreamImpl(ipList: String, accessKey:String,secretKey:String,
                                 val scope: String, val name: String, val config: StreamConfiguration) extends Stream {
  this.logId = new SegmentId(scope, name, 1, 0)
  final private var logId: SegmentId = _
  final private val router: EventRouter = new EventRouter() {
    def getSegmentForEvent(stream: Stream, routingKey: String): SegmentId = logId
  }

  def getSegments(time: Long): StreamSegments = new StreamSegments(Collections.singletonList(logId), time)

  def getLatestSegments: StreamSegments = getSegments(System.currentTimeMillis)

  def getRate(time: Long): Long = 0

  def createProducer[T](s: Serializer[T], config: ProducerConfig): Producer[T] = new ECSProducerImpl[T]( ipList, accessKey,secretKey,this, router, s, config)

  def createConsumer[T](s: Serializer[T], config: ConsumerConfig, startingPosition: Position, l: RateChangeListener): Consumer[T] = {
    null
    // return new ConsumerImpl<>(this, logClient, s, startingPosition,
    // orderer, l, config);
  }

  override def getConfig: StreamConfiguration = this.config

  override def getName: String = this.name
}
