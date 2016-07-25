package com.emc.nautilus.streaming.impl

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.Future

import com.emc.`object`.Protocol
import com.emc.`object`.Protocol._
import com.emc.`object`.s3.jersey.S3JerseyClient
import com.emc.`object`.s3.{S3Client, S3Config}
import com.emc.nautilus.streaming._

/**
  * Created by kandha on 7/23/16.
  */
class ECSProducerImpl[T]( impl: ECSSingleSegmentStreamImpl, router: EventRouter, s: Serializer[T], config: ProducerConfig) extends Producer[T] {

  //Variables required for ECS connections
  //Create Connection to ECS
  val s3config:S3Config


  // client-side load balancing (direct to individual nodes)
  // single VDC
  s3config = new S3Config(HTTP, NODE_IP1, NODE_IP2);
  /* multiple VDCs
  config = new S3Config(HTTP, new Vdc("Boston", VDC1_NODE1, VDC1_NODE2),
    new Vdc("Seattle", VDC2_NODE1, VDC2_NODE2));
  // to enable geo-pinning (hashes the object key and pins it to a specific VDC)
  config.setGeoPinningEnabled(true);
*/
  s3config.withIdentity(S3_ACCESS_KEY_ID).withSecretKey(S3_SECRET_KEY);

  val s3Client: S3Client = new S3JerseyClient(s3config)

  override def publish(routingKey: String, event: T): Future[Void] = {
    //Start ECS Write
    ByteBuffer buff = s.serialize(event)
    s3Client.appendObject(BUCKET_NAME,)


  }

  override def startTransaction(transactionTimeout: Long): Transaction[T] = {

  }

  override def getConfig: ProducerConfig = config

  override def flush(): Unit = {

  }

  override def close(): Unit = {

  }
}
