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

# Recover from Table Segment Pre-Index Full

* [Issue](#issue)
* [Repair Procedure](#repair-procedure)

# Issue

Hash-based Table Segments are used internally to store metadata in Pravega.
One key aspect of the operation of Hash-based Table Segments is that it indexes keys and values in an offline fashion:
first, PUT and/or REMOVE operations are written to Tier-1, and then, the Attribute Index is generated and updated in LTS
as soon as data for that Table Segment is processed and moved to LTS. For more details, please see 
[PDP 48](https://github.com/pravega/pravega/wiki/PDP-48-(Key-Value-Tables-Beta-2)).

The offline approach to build the Attribute Index adopted in Hash-based Table Segments leads to having a backlog of
PUT and/or REMOVE operations that are stored in Tier-1, but not yet indexed and stored in LTS. If LTS is unavailable for
a long period of time and one of such Hash-based Table Segments continues to receive load, there is a risk of having too
much data being accumulated in Tier-1 only. This may be especially problematic as keys and values stored in Tier-1 are also
cached in memory to guarantee consistency, in a process called Table Segment "pre-index" or "tail-caching". Due to this, 
there are a few parameters that limit the amount of non-indexed (i.e., written in Tier-1 but not yet to LTS) data that 
Hash-based Table Segments can tolerate: 
- `tables.preindex.bytes.max` (_defaults to 128MB_): The maximum un-indexed data length for which a Hash-based Table Segment can 
perform tail-caching.
- `tables.recovery.timeout.millis` (_defaults to 60 seconds_): The maximum amount of time to wait for a Table Segment Recovery.
- `tables.unindexed.bytes.max` (_defaults to 128MB_): The maximum allowed un-indexed data length for a Segment. Any updates that 
occur while the amount of un-indexed data exceeds this threshold will be blocked until the amount of un-indexed data is reduced. 
- `tables.systemcritical.unindexed.bytes.max` (_defaults to 256MB_): Same as the previous parameter, but for Metadata
Table Segments (i.e., `container_metadata` and  `storage_metadata` on each Segment Container).

In rare cases in which LTS is super slow or unavailable for hours, but there is some activity happening in a Hash-based
Table Segment, we can find errors in the logs such as:

```
2022-08-08 13:07:57,194 1192230051 [core-19] INFO i.p.s.s.tables.ContainerKeyIndex - KeyIndex[27]: Table Segment 14 cannot perform tail-caching because tail index too long (150542984).
```
```
2022-08-08 13:07:57,194 1192230051 [core-19] INFO i.p.s.s.tables.ContainerKeyIndex - KeyIndex[27]:: System-critical TableSegment 3 is blocked due to reaching max unindexed size.
```

Messages like the ones above indicate that Table Segments may not be able to recover, to write new entries, or both.
If the impacted Hash-based Table Segments are critical for the operation of Pravega, it may leave the impacted Segment
Container unable for processing new operations. Once this situation is reached, recovering the Segment Container requires 
manual intervention.

# Repair Procedure

The repair procedure is relatively simple and works as follows:

1. _Make sure that LTS is available and works properly_: The root cause of the problem is related to LTS not being able
to store data fast enough, or just being unavailable for a very long period of time. Any attempt to recover Pravega
first requires to be sure that LTS is working fine.


2. _Temporarily update Table Segment un-indexed data parameters_: Once LTS is back to working state, we need to increase
the values in the aforementioned parameters. Concretely, the following errors related to the configuration parameters:
i) `...tail index too long.` requires increasing `tables.preindex.bytes.max`, 
ii) `...recovery timed out.` requires increasing `tables.recovery.timeout.millis`, 
iii) `...blocked due to reaching max unindexed size` requires increasing `tables.unindexed.bytes.max` or
`tables.systemcritical.unindexed.bytes.max`, depending on whether the Table Segment is a metadata one (i.e., 
`container_metadata` and `storage_metadata` on each Segment Container) or not.
The values to be set should be enough to get through the blocker issue. For instance, if one Table Segment shows a log
like `cannot perform tail-caching because tail index too long (150542984)`, it is reasonable to configure
`tables.preindex.bytes.max` to 256MBs. However, setting an excessively high value may be problematic in the case that
LTS is still not working properly, as data may continue getting accumulated up to the configured value.
If multiple error messages are found related to Table Segments, you can update all these parameters to higher values.
Also, note that the "pre-index" or "tail-caching" process involves storing data in cache. For this reason, in some 
cases, it may be necessary to also temporarily increase cache size (i.e.,`pravegaservice.cache.size.max`) and Direct Memory 
as well (see [Segment Store Cache Size and Memory Settings](https://cncf.pravega.io/docs/latest/admin-guide/segmentstore-memory)).


3. _Inspect metrics and confirm that un-indexed data is actually being consumed_: Once the parameters have been increased,
we need to be sure that the un-indexed data for Table Segments is being processed. To this end, a recommended procedure
is to keep an eye to the `segmentstore.tablesegment.used_credits` metric. Once you confirm that this metric decreases
for the relevant Table Segments, it is a clear indication that Pravega is moving data to LTS, including the Table Segment 
operations to be indexed. An example of analyzing this metric can be found in [this issue](https://github.com/pravega/pravega/issues/6467). 


4. _Revert changes in Table Segment un-indexed data parameters_: Once all Table Segments have consumed the outstanding
un-indexed data, it is time to revert the configuration changes made to recover the cluster. Otherwise, if we find
again a long-lasting problem with LTS, some Table Segments may accumulate much more data this time, as the bounds
for un-indexed data have been increased (the more accumulated data, the more complex it is to recover the cluster). 