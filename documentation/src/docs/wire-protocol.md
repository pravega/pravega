<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Streaming Service Wire Protocol

This page describes the proposed Wire Protocol for the Streaming Service. See parent page for description of the service as a whole.

Protocol
========

Data is sent over the wire in self-contained "messages" that are either "requests" (messages from the client to the server) or "replies" (which are in response to a request and go back to the client).

All requests and replies have an 8 byte header with two fields, (All data is written in BigEndian format):
1.  Message Type - An integer (4 bytes) identifies the message type and this determines what fields will follow. (Note the protocol can be extended by adding new types)
2.  Length - Unsigned integer 4 bytes (Messages should be &lt; 2^24, but the upper bits remain zero). How many bytes of data from this point forward are part of this message. (Possibly zero, indicating there is no data)

The remainder of the fields are specific to the type of message. A few important messages are listed below.

General
-------

### Partial Message - Request/Reply

1.  Begin/Middle/End - Enum (1 byte)
2.  Data
A partial message is one that was broken up when being sent over the wire. (For any reason). The whole message is reconstructed by reading the partial messages in sequence and assembling them into a whole. It is not valid to attempt to start a new partial message before completing the previous one.

### KeepAlive - Request/Reply

1.  Data - uninterpreted data of the length of the message. (Usually 0 bytes)

Reading
-------

### Read Segment - Request

1.  Segment to read - String (2 byte length, followed by that many bytes of Java's Modified UTF-8)
2.  Offset to read from - Long (8 bytes)
3.  Suggested Length of Reply - int (4 bytes)
    1.  This is how much data the client wants. They won't necessarily get that much.

### Segment Read - Reply

1.  Segment that was read - String (2 byte length, followed by that many bytes of Java's Modified UTF-8)
2.  Offset that was read from - Long (8 bytes)
3.  Is at Tail - Boolean (1 bit)
4.  Is at EndOfSegment - (1 bit)
5.  Data - Binary (remaining length in message)

The client requests to read from a particular stream at a particular offset, it then receives one or more replies in the form of SegmentRead messages. These contain the data they requested (assuming it exists). The server may decided to give the client more or less data than it asked for, in as many replies as it sees fit.

Appending
---------

### Setup Append - Request

1.  ConnectionId - UUID (16 bytes) Identifies this appender.
2.  Segment to append to. - String (2 byte length, followed by that many bytes of Java's Modified UTF-8)

### Append Setup - Reply

1.  Segment that can be appended to. - String (2 byte length, followed by that many bytes of Java's Modified UTF-8)
2.  ConnectionId - UUID (16 bytes) Identifies the requesting appender.
3.  ConnectionOffsetAckLevel - Long (8 bytes) What was the last offset received and stored on this segment for this connectionId (0 if new)

### BeginAppendBlock - Request 

Only valid after SetupAppend has already been done successfully.

1.  ConnectionId - UUID (16 bytes)
2.  ConnectionOffset - Long (8 bytes) Data written so far over this connection to this segment
3.  Length of data before EndAppendBlock message - Integer (4 bytes) 

### EndAppendBlock- Request

1.  ConnectionId - UUID (16 bytes)
2.  ConnectionOffset - Long (8 bytes) Data written so far over this connection
3.  Block Length - (4 Bytes) Total size of the block that was written. (Note this may more or less than the number of bytes between the BeginAppendBlock and this message)

### Event - Request

Only valid inside of a block

1.  Data

### Data Appended - Reply

1.  ConnectionId - UUID (16 bytes)
2.  ConnectionOffsetAckLevel - Long (8 bytes) The highest offset before which all data is successfully stored on this segment for this connectionId

When appending a client

1.  Establishes a connection to what it thinks is the correct host.
2.  Sends a Setup Append request.
3.  Waits for the Append Setup reply.

Then it can
1.  Send a BeginEventBlock request
2.  Send as many messages as can fit in the block
3.  Send an EndEventBlock request

While this is happening the server will be periodically sending it DataAppended replies acking messages. Note that there can be multiple "Appends" setup for a given TCP connection. This allows a client to share a connection when producing to multiple segments.

A client can optimize its appending by specifying a large value in it's BeginAppendBlock message, as the events inside of the block do not need to be processed individually.

The EndEventBlock message specifies the size of the append block rather than the BeginAppendBlock message. This means that the size of the data in the block need not be known in advance. This is useful if a client is producing a stream of small messages. It can begin a block, write many messages and then when it comes time to end the block, it can write a partial message followed the EndAppendBlock message, followed by the remaining partial message. This would avoid having headers on all of the messages in the block without having to buffer them in ram in its process.
