package com.emc.nautilus.streaming.impl;

import java.util.Collections

import com.emc.nautilus.streaming.{Consumer, ConsumerConfig, EventRouter, Position, Producer, ProducerConfig, RateChangeListener, SegmentId, Serializer, Stream, StreamConfiguration, StreamManager, StreamSegments};

/**
 * Created by kandha on 7/21/16.
 */
class DirectToECSStreamManagerImpl(ipList: String, accessKey:String,secretKey:String,
                                   scope: String)  extends StreamManager {

    @Override
    def createStream(streamName: String , config: StreamConfiguration ) : Stream = {
        return new ECSSingleSegmentStreamImpl(ipList, accessKey, secretKey,scope, streamName,config)
    }

    @Override
    def alterStream(streamName: String , config: StreamConfiguration): Unit = {

    }

    @Override
    def getStream(streamName: String) : Stream = null

    @Override
    def close() : Unit = {

    }
}

