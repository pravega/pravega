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
package com.emc.nautilus.streaming.impl

import java.net.URI
import java.nio.ByteBuffer

import com.emc.`object`.Protocol
import com.emc.`object`.Protocol._
import com.emc.`object`.s3.jersey.S3JerseyClient
import com.emc.`object`.s3.{S3Client, S3Config}
import com.emc.nautilus.streaming._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, TimeUnit}
import scala.concurrent.{Await, Future}

/**
  * Created by kandha on 7/23/16.
  */
class ECSProducerImpl[T](ipList: String, accessKey: String, secretKey: String,
                         impl: ECSSingleSegmentStreamImpl, router: EventRouter, s: Serializer[T],
                         config: ProducerConfig) extends Producer[T] {

  //Variables required for ECS connections
  //Create Connection to ECS
  // client-side load balancing (direct to individual nodes)
  // single VDC
  val s3config: S3Config = new S3Config(HTTP, ipList)

  /* multiple VDCs
  config = new S3Config(HTTP, new Vdc("Boston", VDC1_NODE1, VDC1_NODE2),
    new Vdc("Seattle", VDC2_NODE1, VDC2_NODE2));
  // to enable geo-pinning (hashes the object key and pins it to a specific VDC)
  config.setGeoPinningEnabled(true);
*/
  s3config.withIdentity(accessKey).withSecretKey(secretKey)

  val s3Client: S3Client = new S3JerseyClient(s3config)

  override def publish(routingKey: String, event: T): java.util.concurrent.Future[Void] = {
    //Start ECS Write
    val buff: ByteBuffer = s.serialize(event)
    return convert(Future[Unit] {
      {
        val retVal: Long = s3Client.appendObject("pravega", impl.name, buff);
      }
    })
  }

  def convert(x: Future[Unit]): java.util.concurrent.Future[Void] = {
    new java.util.concurrent.Future[Void] {
      override def isCancelled: Boolean = throw new UnsupportedOperationException

      override def get(): Void = {
        Await.result(x, Duration.Inf);
        null
      }

      override def get(timeout: Long, unit: TimeUnit): Void = {
        Await.result(x, Duration.create(timeout, unit))
        null
      }

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = throw new UnsupportedOperationException

      override def isDone: Boolean = x.isCompleted
    }
  }

  override def startTransaction(transactionTimeout: Long): Transaction[T] = {
    null
  }

  override def getConfig: ProducerConfig = config

  override def flush(): Unit = {

  }

  override def close(): Unit = {

  }
}
