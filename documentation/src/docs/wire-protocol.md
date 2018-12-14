<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Streaming Service Wire Protocol

This page describes the proposed Wire Protocol for the Streaming Service. See [Pravega Concepts](http://pravega.io/docs/latest/pravega-concepts) for more information.

# Protocol

Data is sent over the wire in self-contained "messages" that are either "requests" (messages sent from the client to the server) or "replies" (responses sent from the server to the client).

All the requests and replies have 8 byte header with two fields. (All data is written in **BigEndian** format).

- **Message Type**: A 4 byte Integer identifies the message type and determines the subsequent fields. (Note that the protocol can be extended by adding new types.)
- **Length**:  A 4 byte Unsigned Integer (Messages should be less than 2^<sup>24</sup>, but the upper bits remain zero). The remaining bits followed by the upper bit are part of the message. (Possibly zero, indicating there is no data).
The remainder of the fields are specific to the type of the message. A few important messages are listed below.

# General

## Partial Message - Request/Reply

-  **Begin/Middle/End**: Enum (1 byte)
-  **Data**: A partial message is the one that was broken up when the message was sent over the wire. It is essential that for any reason, the whole message is reconstructed by reading the partial message in sequence and reassembled as whole message. It is not valid to attempt to start a new partial message before completing the previous one.

## KeepAlive - Request/Reply

- **Data**: Uninterpreted data of the length of the message. (Usually 0 byte)

# Reading


## Read Segment - Request

1. Segment to read: A length of 2 byte String length, followed by that many bytes of Java's Modified UTF-8.
2. Offset to read from:  8 byte Long.
3. Suggested Length of Reply: 4 byte Integer. The clients can request for the required length to server (but the server may allot a different number of bytes).

## Segment Read - Reply

1.  Segment that was read: A length of 2 byte String, followed by that many bytes of Java's Modified UTF-8).
2.  Offset that was read from: 8 byte Long.
3.  Is at Tail: 1 bit Boolean.
4.  Is at `EndOfSegment`: 1 bit.
5.  Data: Binary (remaining length in the message).

The client requests to read from a particular Stream at a particular Offset. It then receives one or more replies in the form of `SegmentRead` messages. These contain the data they requested (assuming it exists). The server decides on more or less data to give the client than it asked for, in as many replies as it sees fit.

# Appending

## Setup Append - Request

1.  `ConnectionId`: UUID (16 bytes) Identifies this appender.
2.  Segment to append: A length of 2 byte String, followed by that many bytes of Java's Modified UTF-8.

## Append Setup - Reply

1.  Segment that can be appended: A length of 2 byte String, followed by that many bytes of Java's Modified UTF-8.
2.  `ConnectionId`: UUID (16 bytes) Identifies the requesting appender.
3.  `ConnectionOffsetAckLevel`: 8 byte Long. (The last offset received and stored on this Stream Segment for this `ConnectionId` (0 if new)).

## AppendBlock - Request

1. Final `UUID writerId`: (16 bytes).
2. Final `ByteBuf` data: This holds the contents of the block.

## AppendBlockEnd - Request

1. Final `UUID writerId`: (16 bytes).
2. Final `sizeOfWholeEvents`: 4 byte Integer
3. Final `ByteBuf` data: This holds the contents of the block.
4. Final `numEvents`: 4 byte Integer.
5. Final  `lastEventNumber`: 8 byte Long.



## Event - Request

Only valid inside the block.

1.  Data

## Data Appended - Reply

1. Final UUID writerId: (16 bytes)
2. Final `eventNumber`: (8 byte Long).This matches the `lastEventNumber` in the append block.
3. Final `previousEventNumber`: (8 byte Long). This is the previous value of `eventNumber` that was returned in the last `DataAppeneded`.


When appending a client:

- Establishes a connection to the host chosen by it.
- Sends a "Setup Append" request.
- Waits for the "Append Setup" reply.

After receiving the "Append Setup" reply, it performs the following:
- Send a `AppendBlock` request.
- Send as many messages that can fit in the block.
- Send an `AppendBlockEnd` request.

While this is happening, the server will be periodically sending it `DataAppended` replies acking messages. Note that there can be multiple "Appends Setup" for a given TCP connection. This allows a client to share a connection when producing to multiple Segments.

A client can optimize its appending by specifying a large value in it's `AppendBlock` message, as the events inside of the block do not need to be processed individually.

The `ApppendBlockEnd` has a `sizeOfWholeEvents` to allow the append block to be less than full. This allows the client to begin writing a block before it has a large number of events. This avoids the need to buffer up events in the client and allows for lower latency.
