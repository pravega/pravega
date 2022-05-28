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
<span id="_Toc506543504" class="anchor"><span id="_Toc506545512" class="anchor"></span></span>Pravega Controller Service
========================================================================================================================

# Retention in Pravega

## Motivation
Retention is an integral part of any data system. It is important to enable the application to remove their data for at least three reasons:
   - To reclaim storage space
   - To comply with legislation
   - To comply with user requests

We envision in Pravega two broad ways to implement retention:
- Manual: The client API provides a call to truncate the data of a stream at a given point. This point is given by an abstraction that we call a stream cut. A stream cut instance is an opaque object that internally contains a mapping of segments to offsets.
- Automated: A stream can be configured to be truncated automatically based on either time or size.

## How data retention is handled by Pravega
The Controller is responsible for managing retention of data in a stream. 
It does this by periodically truncating a stream at a specific stream cut. 
The choice of a stream cut depends on the retention policy set for the Stream.

### Retention Policy - Size & Time Based

The retention policy for a Pravega stream specifies whether truncation happens based on size or time. 
It contains a minimum limit (min limit) and a maximum limit (max limit) such that the Controller truncates data respecting those limits.
For example, a size-based policy specifies the amount of data to retain, so if a retention policy specifies a minimum limit = 20 GB and a maximum limit = 100 GB, then the truncation cycle ensures that the stream has at least 20 GB and not more than 100 GB.

### Retention Set
The Controller on every truncation cycle generates a Stream Cut SC at the tail end of the stream and stores SC in a set called the retention set with the stream metadata. A Stream Cut SC from the retention set is chosen to truncate the Stream.

### Retention Service
A retention service runs a retention job periodically (default 30 mins) on Controller to check if a Stream needs to be truncated as per the configuration policy. 
When attempting to truncate a stream, the Controller chooses a stream cut SC from the Retention Set and the stream is truncated at SC. 
Stream truncation results in the stream having a new head stream cut which is the same as the stream cut chosen for truncation.

The retention workflow performs the following for each stream S: 
 - If S has a retention policy set, consider it for truncation.
 - Generate a Tail Stream Cut, TSC near the tail end of the Stream where writers could still be appending data.
 - Add TSC to the Retention Set for the Stream.
 - Compute the current size of the Stream from Head Stream Cut HSC to TSC.
 - Based on size of the Stream, arrive at a truncation stream cut using the following algorithm:
 - If the size of S is less than min limit, then do nothing.
 - If size of S is greater than min limit and max limit is not set, find a Stream Cut SC closest to but greater than the min limit, such that truncating at SC leaves more data in S than min limit.
 - If a max limit is set, and the size of S is greater than the max limit, find a Stream Cut SC in the retention set closest to but smaller than the max limit, such that truncating at SC leaves less data than the max limit but more data than the min limit in S.
 - If there is a max limit set and the size of S is between the max and the min limits, then attempt to truncate S at a Stream Cut SC closest to its min limit.

Stream S with Segments S1 to S12 and Stream Cuts SC1, SC2, SC3, SC4, SC5, SC6.

### Consumption Based Retention

#### Need

Size and time based retention policies are suitable for applications that need to store data over the longer term (archival). 
In some scenarios, however, streaming data is transient (short lived), or storage space is at a premium; consequently, reclaiming storage space as soon as applications no longer need the data is highly desirable in such scenarios.
Use cases that require such space reclamation include but are not limited to the following:
1. A message queue - When using a Pravega Stream as a message queue, events in the queue can be deleted post consumption by all subscribers.
2. Deployments with limited storage capacity - The storage available on small-footprint environments like edge devices (gateways) is typically constrained. Once data is no longer needed, perhaps because it has moved out of the edge to a core data center or to the cloud, it can be deleted to create space for more incoming data.
   
#### How it works
Consumption-Based Retention aims to reclaim space occupied by consumed data in a stream. It retains data for as long as at least one consuming application has not consumed it. Like the other two retention policies, it relies on stream cuts to determine positions to truncate the Stream.

For a given stream S, each application consuming data from S regularly publishes a Stream Cut SC corresponding to its consumed position to the Controller. 
The Stream Cut SC serves as an acknowledgement that all data prior to this position in the stream has been consumed and the application no longer needs that data. We call SC an Acknowledgement Stream Cut. Upon receiving published stream cuts, the Controller stores them with the metadata of S. The controller periodically runs a workflow to find streams that are eligible for truncation. When running this workflow, if the metadata of S has an acknowledgement stream cut from at least one application, the workflow truncates this stream according to consumption. Otherwise, it falls back to any configured space or time-based policy.

The acknowledgement stream cut for each subscriber reader group is stored with the stream metadata.
The Controller stores only the most recent stream cut for each Subscriber.

![Stream Metadata](img/figure-table.png)

When the retention workflow runs, and a Stream S has a retention policy set, we first check if the metadata table for S has any subscriber stream cuts.
1. If no entries are found, the stream is truncated based on space or time limits, depending on the retention policy configuration for the stream.
2. If only a single subscriber stream cut is present, we consider this stream cut as the subscriber_lower_bound stream cut.
3. If the acknowledgement stream cuts for multiple subscribers are present, we compute a subscriber_lower_bound stream cut based on stream cuts of all subscribers. The subscriber_lower_bound is computed such that truncating at this stream cut ensures that no subscriber loses any unconsumed data. The algorithm for arriving at a subscriber_lower_bound stream cut is out of scope for this document.
   On Controller, the truncation process, happens as follows:

   1) Compute a subscriber_lower_bound stream cut, using acknowledgement stream cuts of all consuming applications. 
   Truncating the stream at this point would ensure no application loses unconsumed data. 
   2) Once we have a subscriber_lower_bound stream cut, the stream can be truncated based on the following:

       a. subscriber_lower_bound stream cut is within min & max limits: If truncating at the subscriber_lower_bound stream cut leaves more data in the stream than the configured minimum limit, but less data than the configured maximum limit, as per the stream retention policy, we choose to truncate the stream at the subscriber_lower_bound s stream cut.

       b. subscriber_lower_bound is less than Min Limit: If truncating at the subscriber_lower_bound, leaves less data in the stream than required by the min limit in the retention policy, then we discard the subscriber_lower_bound. We find another stream cut closest to but greater than the min limit, such that truncating at this would leave more data in the Stream than the min limit.

       c. subscriber_lower_bound is greater than Max Limit: If truncating at the subscriber_lower_bound would leave more data in the stream than max limit, we discard subscriber_lower_bound. Instead, we find a stream cut closest to the max limit, such that truncating at this Stream Cut would leave less data in the Stream than the max limit.

       d. subscriber_lower_bound Overlaps with Min Limit: In this case, we choose to truncate the Stream at the first non-overlapping stream cut preceding the subscriber_lower_bound stream cut from the tail. This way we may leave a little more
data in the Stream than is required by Consumption Based Retention, but we guarantee that we satisfy the minimum limit specified by the retention policy.

       e. subscriber_lower_bound Overlaps with Max Limit: In this case, we choose to truncate the stream at the first non-overlapping stream cut immediately succeeding the
subscriber_lower_bound Stream Cut from the head.

Note: Stream truncation happens asynchronously and eventually based on the truncation Stream-Cuts published by all Subscribers and min/max limits per the Retention Policy.
Enabling Consumption Based Retention on a Pravega Stream
To enable consumption-based retention the following must hold:
▪ The stream must have a retention policy configured.
▪ One or more reader groups reading from the Stream should be subscriber reader groups.
▪ The Controller should periodically receive acknowledgement stream cuts from the subscriber reader groups, indicating their consumption boundary in the Stream.
Creating a Subscriber Reader Group
A reader group interested in subscribing is configured accordingly. We add a new Retention Configuration field in the reader group configuration to indicate that a reader group is either a subscriber or non-subscriber. The configuration also indicates whether the reader group publishes acknowledgement stream cuts to the Controller automatically or not. The configuration for acknowledgement stream cuts can have the following values:
▪ Auto_Publish_At_Last_Checkpoint - A Subscriber Reader Group would automatically
publish the Stream Cut corresponding to the last read checkpoint to Controller.
▪ Manual_Publish_Stream_Cut – A Subscriber Reader Group would not automatically
publish Stream Cuts to Controller but instead the user application would need to explicitly invoke readerGroup.updateTruncationStreamCut() and provide the acknowledgement Stream Cut to be updated to Controller.
▪ None – A non-subscriber Reader Group.
A Reader Group can be converted between a subscriber to non-subscriber or vice versa by changing the
corresponding value of the retention policy in the reader group configuration.
Acknowledging Consumed Positions Using Checkpoints
There are two ways for an application to acknowledge its consumed position: automatically via checkpoints or explicitly by generating a stream cut and publishing to the Controller.
Automatically Publish Acknowledgement Stream Cuts
Reader groups checkpoint either automatically or via explicit API calls. A checkpoint consists of coordinating the position across all readers in the group and all segments they are reading from, at checkpoint time. Once a reader learns of an ongoing checkpoint (internally via shared reader group state), it emits a checkpoint event. The checkpoint event for a given reader in the group separates the data events before the checkpoint and events after the checkpoint. Reading a data event after the checkpoint event indicates that the corresponding reader has read and consumed all events before the checkpoint. Once all readers read beyond their corresponding checkpoint events, the stream cut corresponding to the position of the checkpoint is published to the Controller as an acknowledgement for the consumed position of this Reader Group.
Explicit Publishing of Acknowledgement Stream Cuts by the Application
Alternatively, checkpoints can be generated on the stream by the user application. The application can choose to publish the stream cut corresponding to this checkpoint to the Controller, once it determines that it does not need data in the stream prior to this checkpoint position.
Consumption Based Retention Sequence Diagram
Figure 2 shows the interaction among various Pravega components required to implement Consumption based retention. In this figure, RG1 and RG2 are 2 different subscriber reader groups reading from Stream S1 and these publish stream cuts SC1 and SC2 respectively to indicate their consumed positions in the Stream.

 
 
 
 
 
 


