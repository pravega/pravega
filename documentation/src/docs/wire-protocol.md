<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Pravega Streaming Service Wire Protocol

This page describes the proposed Wire Protocol for the Streaming Service. See [Pravega Concepts](https://pravega.io/docs/latest/pravega-concepts) for more information.

# Protocol

Data is sent over the wire in self-contained "messages" that are either "requests" (messages sent from the client to the server) or "replies" (responses sent from the server to the client).

All the requests and replies have 8 byte headers with two fields (all data is written in **BigEndian** format).

- **Message Type**: An Integer (4 bytes) identifies the message type and determines the subsequent fields. (Note that the protocol can be extended by adding new types.)
- **Length**:  An Integer (4 bytes) (Messages should be less than 2<sup>24</sup>, but the upper bits remain zero). Payload size of the message (possibly zero, indicating there is no data). The remainder of the fields are specific to the type of the message. A few important messages are listed below.

## Protocol Primitive Types

The protocol is built out of the following primitive types.

| **Type** | **Description** |
|----------|--------------|
|BOOLEAN (1 byte)| Values 0 and 1 are used to represent _False_ and _True_ respectively. When reading a boolean value, any non-zero value is considered true.|
|STRING (2 bytes)|A sequence of characters. The first 2 bytes are used to indicate the byte length of the UTF-8 encoded character sequence, which is non-negative. This is followed by the UTF-8 encoding of the string.|
|VARLONG (8 bytes)|An Integer between -2<sup>63</sup> and 2<sup>63</sup>-1 inclusive. Encoding follows the variable-length zig-zag encoding from [Google Protocol Buffers](https://developers.google.com/protocol-buffers/).|
|INT (4 bytes)|An Integer between -2<sup>31</sup> and 2<sup>31</sup>-1 inclusive.|
|UUID (16 bytes)|Universally Unique Identifiers (UUID) as defined by RFC 4122, ISO/IEC 9834-8:2005, and related standards. It can be used as a global unique 128-bit identifier.|


# Reading

## Read Segment - Request

| **Field**   |**Datatype**   | **Description**     |
|-------------|------------|----------|
|  `Segment`| String| The Stream Segment that was read. |
| `Offset`   | Long| The `Offset` in the Stream Segment to read from. |
| `suggestedLength` of Reply|Integer|The clients can request for the required length to the server (but the server may allot a different number of bytes.|
|`delegationToken`|String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|
|`RequestId`| Long| The client-generated _ID_ that identifies a client request.|

More information on `Segment` Request messages like `MergeSegment`, `SealSegment`, `TruncateSegment` and `DeleteSegment`, can be found [here](https://github.com/pravega/pravega/blob/master/shared/protocol/src/main/java/io/pravega/shared/protocol/netty/WireCommands.java).


## Segment Read - Reply

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|--------|
| `Segment`|String| This Segment indicates the Stream Segment that was read.|
|`Offset`|Long| The `Offset` in the Stream Segment to read from.|
|`Tail`|Boolean| If the read reached the tail of the Stream Segment.|
| `EndOfSegment`| Boolean| If the read reached the end of the Stream Segment.|
| `Data`| Binary| Remaining length in the message.|
|`RequestId`| Long| The client-generated _ID_ that identifies a client request.|

The client requests to read from a particular Segment at a particular `Offset`. It then receives one or more replies in the form of `SegmentRead` messages. These contain the data they requested (assuming it exists). The server may decide transferring to the client more or less data than it was asked for, splitting that data in a suitable number of reply messages.

More information on `Segment` Reply messages like `SegmentIsSealed`,`SegmentIsTruncated`, `SegmentAlreadyExists`,`NoSuchSegment` and `TableSegmentNotEmpty`, can be found [here](https://github.com/pravega/pravega/blob/master/shared/protocol/src/main/java/io/pravega/shared/protocol/netty/WireCommands.java).

# Appending

## Setup Append - Request

| **Field**      | **Datatype**|**Description**     |
|-------------|----------|---------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
| `writerId`|UUID| Identifies the requesting appender.|
| `Segment`| String| This Segment indicates the Stream Segment that was read.|
| `delegationToken`| String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|

## Append Setup - Reply

| **Field**      |**Datatype** | **Description**     |
|-------------|----------|---------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
|  `Segment`| String| This Segment indicates the Stream Segment to append to.|
|  `writerId`| UUID| Identifies the requesting appender. This ID is used to identify the Segment for which an AppendBlock is destined.|
|  `lastEventNumber`| Long| Specifies the last event number in the Stream.|

## AppendBlock - Request

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|---------|
| `writerId`| UUID | Identifies the requesting appender.|
| `Data`| Binary| This holds the contents of the block.|
|`RequestId`| Long| The client-generated _ID_ that identifies a client request.|

## AppendBlockEnd - Request

| **Field**      | **Datatype** | **Description**     |
|-------------|----------|--------|
| `writerId`| UUID | Identifies the requesting appender.|
| `sizeOfWholeEvents`| Integer | The total number of bytes in this block (starting from the beginning) that is composed of whole (meaning non-partial) events.|
| `Data`| Binary| This holds the contents of the block.|
| `numEvents`| Integer | Specifies the current number of events.|
| `lastEventNumber`| Long | Specifies the value of last event number in the Stream.|
|`RequestId`| Long | The client-generated _ID_ that identifies a client request.|

The `ApppendBlockEnd` has a `sizeOfWholeEvents` to allow the append block to be less than full. This allows the client to begin writing a block before it has a large number of events. This avoids the need to buffer up events in the client and allows for lower latency.

## Partial Event - Request/Reply

-  **Data**: A Partial Event is an Event at the end of an Append block that did not fully fit in the Append block. The remainder of the Event will be available in the `AppendBlockEnd`.

## Event - Request

| **Field**      | **Description**     |
|-------------|----------|
| `Data`| Specifies the Event's data (only valid inside the block).|

## Data Appended - Reply

| **Field**      | **Datatype**| **Description**     |
|-------------|----------|--------|
| `writerId`| UUID| Identifies the requesting appender.|
| `eventNumber`|Long | This matches the `lastEventNumber` in the append block.|
| `previousEventNumber`| Long | This is the previous value of `eventNumber` that was returned in the last `DataAppeneded`.|
|`RequestId`| Long | The client-generated _ID_ that identifies a client request.|

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

# Segment Attribute

## GetSegmentAttribute - Request

| **Field**    |**Datatype**  | **Description**     |
|-------------|----------|------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
|  `SegmentName`| String| The Segment to retrieve the attribute from.|
|  `attributeId`| UUID| The attribute to retrieve.|
| `delegationToken`| String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|

## SegmentAtrribute - Reply

| **Field**    |**Datatype**  | **Description**     |
|-------------|----------|------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
| `Value`|Long|The value of the attribute.|

More information on `SegmentAttribute` Request message like `updateSegmentAttribute` and Reply message like `SegmentAttributeUpdate` can be found [here](https://github.com/pravega/pravega/blob/master/shared/protocol/src/main/java/io/pravega/shared/protocol/netty/WireCommands.java).

# TableSegment

## ReadTable - Request

| **Field**    |**Datatype**  | **Description**     |
|-------------|----------|------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
|  `Segment`| String| The Stream Segment that was read. |
|`delegationToken`|String| This was added to perform _auth_. It is an opaque-to-the-client token provided by the Controller that says it's allowed to make this call.|
|`keys`|List<TableKey>|The version of the key is always set to `io.pravega.segmentstore.contracts.tables.TableKey.NO_VERSION`.|

More information on `TableSegments` Request messages like `MergeTableSegments`, `SealTableSegment`, `DeleteTableSegment`, `UpdateTableEntries`, `RemoveTableKeys`, `ReadTableKeys` and `ReadTableEntries` can be found [here](https://github.com/pravega/pravega/blob/master/shared/protocol/src/main/java/io/pravega/shared/protocol/netty/WireCommands.java).

## TableRead - Reply

| **Field**    |**Datatype**  | **Description**     |
|-------------|----------|------|
| `RequestId`| Long| The client-generated _ID_ that identifies a client request.|
| `Segment`| String| The Stream Segment that was read. |
|`Entries`|TableEntries| The entries of the Table that was read.|

More information on `TableSegments` Reply messages like `TableEntriesUpdated`, `TableKeysRemoved`, `TableKeysRead` and `TableEntriesRead` can be found [here](https://github.com/pravega/pravega/blob/master/shared/protocol/src/main/java/io/pravega/shared/protocol/netty/WireCommands.java).
