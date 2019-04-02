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

## Protocol Primitive Types

The protocol is built out of the following primitive types.

| **Type** | **Description** |
|----------|--------------|
|BOOLEAN (1 bit)|Represents a boolean value in a byte. Values 0 and 1 are used to represent false and true respectively. When reading a boolean value, any non-zero value is considered true.|
|STRING (2 bytes)|Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. Length must not be negative.|
|VARLONG (8 bytes)|Represents an integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.|
|INT (4 bytes)|Represents an integer between -2<sup>3</sup> and 2<sup>3</sup>-1 inclusive.|
|UUID (16 bytes)|Universally Unique Identifiers (UUID) as defined by RFC 4122, ISO/IEC 9834-8:2005, and related standards. It can be used as a global unique 128-bit identifier.|


# Reading

## Read Segment - Request

| **Field**   |**Datatype**   | **Description**     |
|-------------|------------|----------|
|  `Segment`| String| This Segment indicates the Stream Segment that was read. |
| `Offset`   | Long| The `Offset` in the Stream Segment to read from. |
| `suggestedLength` of Reply|Integer|. The clients can request for the required length to the server (but the server may allot a different number of bytes.|
|`delegationToken`|String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|
|`RequestId`| Long| This field contains the client-generated _ID_ that has been propagated to identify a client request.|


## Segment Read - Reply

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|--------|
| `Segment`|String| This Segment indicates the Stream Segment that was read.|
|`Offset`|Long| The `Offset` in the Stream Segment to read from.|
|`Tail`|Boolean| If the read was performed at the tail of the Stream Segment.|
| `EndOfSegment`| Boolean| If the read was performed at the end of the Stream Segment.|
| `Data`| Binary| Remaining length in the message.|
|`RequestId`| Long| This field contains the client-generated _ID_ that has been propagated to identify a client request.|

The client requests to read from a particular Segment at a particular `Offset`. It then receives one or more replies in the form of `SegmentRead` messages. These contain the data they requested (assuming it exists). The server may decide transferring to the client more or less data than it was asked for, splitting that data in a suitable number of reply messages.

### Segment API

| **API**      | **Description**     |
|-------------|----------|
|`SegmentIsSealed`|The requested Segment is Sealed.|
|`SegmentIsTruncated`|-`Start offset`: Represents the offset at which the Segment is Truncated.|
|`SegmentAlreadyExists`|The requested Segment already exists.|
|`NoSuchSegment`| The requested Segment do not exist.|
|`TableSegmentNotEmpty`||


# Appending

## Setup Append - Request

| **Field**      | **Datatype**|**Description**     |
|-------------|----------|
| `RequestId`| Long| This field contains the client-generated _ID_ that has been propagated to identify a client request.|
| `writerId`|UUID| It identifies the requesting appender.|
| `Segment`| String| This Segment indicates the Stream Segment that was read.|
| `delegationToken`| String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|

## Append Setup - Reply

| **Field**      |**Datatype** | **Description**     |
|-------------|----------|---------|
| `RequestId`| Long| This field contains the client-generated ID that has been propagated to identify a client request.|
|  `Segment`| String| This Segment indicates the Stream Segment to append.|
|  `writerId`| UUID| It identifies the requesting appender.|
|  `lastEventNumber`| Long| It specifies the last event number in the Stream.|

## AppendBlock - Request

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|---------|
| `writerId`| UUID | It identifies the requesting appender.|
| `Data`| Binary| This holds the contents of the block.|
|`RequestId`| Long| This field contains the client-generated _ID_ that has been propagated to identify a client request.|

## AppendBlockEnd - Request

| **Field**      | **Datatype** | **Description**     |
|-------------|----------|--------|
| `writerId`| UUID | It identifies the requesting appender.|
| `sizeOfWholeEvents`| Integer | It is the total number of bytes in this block (starting from the beginning) that is composed of whole (meaning non-partial) events.|
| `Data`| Binary| This holds the contents of the block.|
| `numEvents`| Integer | It specifies the current number of events.|
| `lastEventNumber`| Long | It specifies the value of last event number in the Stream.|
|`RequestId`| Long | This field contains the client-generated _ID_ that has been propagated to identify a client request.|

The `ApppendBlockEnd` has a `sizeOfWholeEvents` to allow the append block to be less than full. This allows the client to begin writing a block before it has a large number of events. This avoids the need to buffer up events in the client and allows for lower latency.

## Partial Event - Request/Reply

-  **Data**: A Partial Event is an Event at the end of an Append block that did not fully fit in the Append block. The remainder of the Event will be available in the `AppendBlockEnd`.

## Event - Request

| **Field**      | **Description**     |
|-------------|----------|
| `Data`| It contains the Event's data (only valid inside the block).|

## Data Appended - Reply

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|--------|
| `writerId`| UUID| It identifies the requesting appender.|
| `eventNumber`|Long | This matches the `lastEventNumber` in the append block.|
| `previousEventNumber`| Long | This is the previous value of `eventNumber` that was returned in the last `DataAppeneded`.|
|`RequestId`| Long | This field contains the client-generated _ID_ that has been propagated to identify a client request.|

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

| **API**      | **Description**     |
|-------------|----------|
|`GetSegmentAttribute`| The requested list of Segment attributes.|
|`UpdateSegmentAttribute`|  - `newValue`: (Long). It represents the new value to be updated.|
| |                         - `expectedValue`: (Long). It represents |

## Table Segment API

| **API**      | **Description**     |
|-------------|----------|
|`CreateTableSegment`| To create Table Segment.|
|`MergeTableSegment`| - `Target`: (String). This indicates the target of the Stream Segment to be Merged.|
|                    |- `Source`: (String). This indicates the source Stream Segment that needs to be Merged.|
|`MergeSegment`|Performs merging of Segments.|
|`SealSegment`| Performs sealing of Segments.|
|`SealTableSegment`| Performs sealing of Table Segments.|
|`TruncateSegment`| `TruncationOffset`: (Long). This contains the Offset for the Segment to be Truncated.|
|`DeleteSegment`| Performs deletion of created Segments.|
|`DeleteTableSegment`|`mustBeEmpty` (Boolean): If true, the Table Segment will only be deleted if it is empty (contains no keys).|

## Table API

| **API**      | **Description**     |
|-------------|----------|
|`TableEntries`| `updatedVersions`: (List<long>). List of Table entries updated.|
|`RemoveTableKeys`| `keys`: (List<TableKey>). List of Table Keys removed.|
|`ReadTable` |  `keys`: (List<TableKey>). The version of the key is always set to `io.pravega.segmentstore.contracts.tables.TableKey.NO_VERSION`|
|`ReadTableKeys`|- `suggestedKeyCount`: (Integer). Suggested number of `{@link TableKey}`s to be returned by the Segment Store.
|
|               |- `continuationToken`: (Byte Buf). This is used to indicate the point from which the next keys should be fetched.|
|`ReadTableEntries`|- `suggestedEntryCount`: (Integer). Suggested number of `{@link TableKey}`s to be returned by the Segment Store.
.|
|                  |- `continuationToken`: (ByteBuf). This is used to indicate the point from which the next entry should be fetched.|
