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

# Reading

## Read Segment - Request

| **Field**      | **Description**     |
|-------------|----------|
|  `Segment`|String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This Segment indicates the Stream Segment that was read. |
| `Offset`   | Long (8 bytes). The `Offset` in the Stream Segment to read from. |
| `suggestedLength` of Reply|Integer (4 bytes). The clients can request for the required length to the server (but the server may allot a different number of bytes.|
|`delegationToken`|String (2 byte) followed by that many bytes of Java's Modified UTF-8. This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|


## Segment Read - Reply

| **Field**      | **Description**     |
|-------------|----------|
| `Segment`|String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This Segment indicates the Stream Segment that was read.|
|`Offset`|Long (8 bytes). The `Offset` in the Stream Segment to read from.|
|`Tail`|Boolean (1 bit). If the read was performed at the tail of the Stream Segment.|
| `EndOfSegment`| Boolean (1 bit). If the read was performed at the end of the Stream Segment.|
| `Data`| Binary (remaining length in the message).|

The client requests to read from a particular Segment at a particular `Offset`. It then receives one or more replies in the form of `SegmentRead` messages. These contain the data they requested (assuming it exists). The server may decide transferring to the client more or less data than it was asked for, splitting that data in a suitable number of reply messages.

### Segment API

The follwoing Paramters will be used by all the below mentioned APIs.

- `RequestId`: Long (8 bytes). This field contains the client-generated _ID_ that has been propagated to identify a client request.
- `Segment`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This Segment indicates the Stream Segment that was read.
- `ServerStackTrace`: String (2 bytes)  


| **API**      | **Description**     |
|-------------|----------|
|`SegmentIsSealed`|The requested Segment is Sealed.|
|`SegmentIsTruncated`|-`Start offset`: Represnts the offset at whiich the Segment is Truncated.|
|`SegmentAlreadyExists`|The requested Segment Already exists.|
|`NoSuchSegment`| The requested Segment do not exist.|
|`TableSegmentNotEmpty`||


# Appending

## Setup Append - Request

| **Field**      | **Description**     |
|-------------|----------|
| `RequestId`| Long (8 bytes). This field contains the client-generated _ID_ that has been propagated to identify a client request.|
| `writerId`| UUID (16 bytes). It identifies the requesting appender.|
| `Segment`| String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This Segment indicates the Stream Segment that was read.|
| `delegationToken`| String (2 byte) followed by that many bytes of Java's Modified UTF-8. This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|

## Append Setup - Reply

| **Field**      | **Description**     |
|-------------|----------|
| `RequestId`| Long (8 bytes). This field contains the client-generated ID that has been propagated to identify a client request.|
|  `Segment`| String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This Segment indicates the Stream Segment to append.|
|  `writerId`| UUID (16 bytes). It identifies the requesting appender.|
|  `lastEventNumber`| Long (8 bytes). It specifies the last event number in the Stream.|

## AppendBlock - Request

| **Field**      | **Description**     |
|-------------|----------|
| `writerId`| UUID (16 bytes). It identifies the requesting appender.|
| `Data`| This holds the contents of the block.|

## AppendBlockEnd - Request

| **Field**      | **Description**     |
|-------------|----------|
| `writerId`| UUID (16 bytes). It identifies the requesting appender.|
| `sizeOfWholeEvents`| Integer (4 bytes). It is the total number of bytes in this block (starting from the beginning) that is composed of whole (meaning non-partial) events.|
| `Data`| This holds the contents of the block.|
| `numEvents`| Integer (4 bytes). It specifies the current number of events.|
| `lastEventNumber`| Long (8 bytes). It specifies the value of last event number in the Stream.|

The `ApppendBlockEnd` has a `sizeOfWholeEvents` to allow the append block to be less than full. This allows the client to begin writing a block before it has a large number of events. This avoids the need to buffer up events in the client and allows for lower latency.

## Partial Event - Request/Reply

-  **Data**: A Partial Event is an Event at the end of an Append block that did not fully fit in the Append block. The remainder of the Event will be available in the `AppendBlockEnd`.

## Event - Request

| **Field**      | **Description**     |
|-------------|----------|
| `Data`| It contains the Event's data (only valid inside the block).|

## Data Appended - Reply

| **Field**      | **Description**     |
|-------------|----------|
| `writerId`| UUID (16 bytes).It identifies the requesting appender.|
| `eventNumber`|Long (8 bytes). This matches the `lastEventNumber` in the append block.|
| `previousEventNumber`| Long (8 bytes). This is the previous value of `eventNumber` that was returned in the last `DataAppeneded`.|

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

## Segment Attribute API

The following Paramters will be used by all the below mentioned APIs.

- `RequestId`: Long (8 bytes). This field contains the client-generated _ID_ that has been propagated to identify a client request.
- `SegmentName`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This indicates the name of the Stream Segment.
- `attributeId`: UUID (16 bytes).It identifies the requested attributeId.  


| **API**      | **Description**     |
|-------------|----------|
|`GetSegmentAttribute`| The requested list of segment attributes.|
|`UpdateSegmentAttribute`|  - `newValue`: Long (8 bytes). It represents the new value to be updated.|
| |                         - `expectedValue`: Long (8 bytes). It represents |

## Table Segment API

The following Paramters will be used by all the below mentioned APIs.

- `RequestId`: Long (8 bytes). This field contains the client-generated _ID_ that has been propagated to identify a client request.
- `SegmentName`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This indicates the name of the Stream Segment.
- `delegationToken`: String (2 byte) followed by that many bytes of Java's Modified UTF-8. This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.


| **API**      | **Description**     |
|-------------|----------|
|`CreateTableSegment`| To create Table segment.|
|`MergeTableSegment`| - `Target`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This indicates the target of the Stream Segment to be Merged.|
|                    |- `Source`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This indicates the source Stream Segments that needs to be Merged.|
|`MergeSegment`|Perfomrs merging of Segments.|
|`SealSegment`| Perfoms sealing of Segments.|
|`SealTableSegment`| Perfoms sealing of Table Segments.|
|`TruncateSegment`| `TruncationOffset`: Long (8 bytes). This contains the Offset for the segment to be Truncated.|
|`DeleteSegment`| Perfoms deletion of created segments.|
|`DeleteTableSegment`|`mustBeEmpty` (Boolean): If true, the Table Segment will only be deleted if it is empty (contains no keys).|

## Table API

The following Paramters will be used by all the below mentioned APIs.

- `RequestId`: Long (8 bytes). This field contains the client-generated _ID_ that has been propagated to identify a client request.
- `SegmentName`: String (2 bytes) followed by that many bytes of Java's Modified UTF-8. This indicates the name of the Stream Segment.
- `delegationToken`: String (2 byte) followed by that many bytes of Java's Modified UTF-8. This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.
-`TableEntries` : tableEntries;


| **API**      | **Description**     |
|-------------|----------|
|`TableEntries`| `updatedVersions`: (List<long>). List of Table entried updated.|
|`RemoveTableKeys`| `keys`: (List<TableKey>). List of Table Keys removed.|
|`ReadTable` |  `keys`: (List<TableKey>). The version of the key is always set to `io.pravega.segmentstore.contracts.tables.TableKey.NO_VERSION`|
|`ReadTableKeys`|- `suggestedKeyCount`: (int).|
|               |- `continuationToken`: (Byte Buf). This is used to indicate the point from which the next keys should be fetched.|
|`ReadTableEntries`|- `suggestedEntryCount`: (int)
|                  |- `continuationToken`: (ByteBuf). This is used to indicate the point from which the next entry should be fetched.|
