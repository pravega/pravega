<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Streaming Service Wire Protocol

This page describes the proposed Wire Protocol for the Streaming Service. See [Pravega Concepts](http://pravega.io/docs/latest/pravega-concepts) for more information.

# Protocol

Data is sent over the wire in self-contained "messages" that are either "requests" (messages sent from the client to the server) or "replies" (responses sent from the server to the client).

All the requests and replies have 8 byte headers with two fields (all data is written in **BigEndian** format).

- **Message Type**: An Integer (4 bytes) identifies the message type and determines the subsequent fields. (Note that the protocol can be extended by adding new types.)
- **Length**:  An Integer (4 bytes) (Messages should be less than 2<sup>24</sup>, but the upper bits remain zero). Payload size of the message (possibly zero, indicating there is no data). The remainder of the fields are specific to the type of the message. A few important messages are listed below.

# General

## KeepAlive - Request/Reply

# Reading

## Read Segment - Request

1. Segment to read: String (2 byte length, followed by that many bytes of Java's Modified UTF-8).
2. Offset to read from: Long (8 bytes).
3. Suggested Length of Reply: Integer (4 bytes). The clients can request for the required length to server (but the server may allot a different number of bytes).
4. `delegationToken`: This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.


## Segment Read - Reply

1.  Segment that was read: String (2 byte length, followed by that many bytes of Java's Modified UTF-8).
2.  Offset that was read from: Long (8 bytes).
3.  Is at Tail: Boolean (1 bit).
4.  Is at `EndOfSegment`: 1 bit.
5.  `Data`: Binary (remaining length in the message).

The client requests to read from a particular Segment at a particular Offset. It then receives one or more replies in the form of `SegmentRead` messages. These contain the data they requested (assuming it exists). The server may decide transferring to the client more or less data than it was asked for, splitting that data in a suitable number of reply messages.

# Appending

## Setup Append - Request
1.  `RequestId`: Long (8 bytes). (This field contains the client-generated ID that has been propagated to identify a client request).
2.  `writerId`: UUID (16 bytes) identifies the requesting appender.
3.  Segment to append: String (2 byte length, followed by that many bytes of Java's Modified UTF-8).
4. `delegationToken`: This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.

## Append Setup - Reply

1.  `RequestId`: Long (8 bytes). (This field contains the client-generated ID that has been propagated to identify a client request).
2.  Segment to append: String (2 byte length, followed by that many bytes of Java's Modified UTF-8).
3.  `writerId`: UUID (16 bytes) identifies the requesting appender.
4.  `lastEventNumber`: Long (8 bytes). Specifies the last event number in the Stream.

## AppendBlock - Request

1. `writerId`: UUID (16 bytes) identifies the requesting appender.
2. `Data`: This holds the contents of the block.

## AppendBlockEnd - Request

1. `writerId`: UUID (16 bytes) identifies the requesting appender.
2. `sizeOfWholeEvents`: Integer (4 bytes).
3. `Data`: This holds the contents of the block.
4. `numEvents`: Integer (4 bytes). Specifies the current number of events.
5. `lastEventNumber`: Long (8 bytes). Specifies the value of last event number in the Stream.


## Partial Event - Request/Reply

-  **Data**: A partial message is the one that was broken up when the message was sent over the wire. It is essential that for any reason, the whole message is reconstructed by reading the partial message in sequence and reassembled as whole message. It is not valid to attempt to start a new partial message before completing the previous one.


## Event - Request

Only valid inside the block.

1.  `Data`

## Data Appended - Reply

1. `writerId`: UUID (16 bytes) identifies the requesting appender.
2. `eventNumber`: Long (8 bytes). This matches the `lastEventNumber` in the append block.
3. `previousEventNumber`: Long (8 bytes). This is the previous value of `eventNumber` that was returned in the last `DataAppeneded`.


When appending a client:

- Establishes a connection to the host chosen by it.
- Sends a "Setup Append" request.
- Waits for the "Append Setup" reply.

After receiving the "Append Setup" reply, it performs the following:
- Send a `AppendBlock` request.
- Send as many Events that can fit in the block.
- Send an `AppendBlockEnd` request.

While this is happening, the server will be periodically sending it `DataAppended` replies acking messages. Note that there can be multiple "Appends Setup" for a given TCP connection. This allows a client to share a connection when producing to multiple Segments.

A client can optimize its appending by specifying a large value in it's `AppendBlock` message, as the events inside of the block do not need to be processed individually.

The `ApppendBlockEnd` has a `sizeOfWholeEvents` to allow the append block to be less than full. This allows the client to begin writing a block before it has a large number of events. This avoids the need to buffer up events in the client and allows for lower latency.
