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

# Enable Durable Data Log

* [Issue](#issue)
* [Repair Procedure](#repair-procedure)

# Issue
A `DurableDataLog` is the abstraction that the Segment Store uses to interface with the specifics of
writing to Tier-1 (i.e., Bookkeeper). As visible in that interface, a `DurableDataLog` can be _enabled_ 
(writable) or _disabled_ (non-writable). In general, when a log becomes _disabled_, it means that there 
has been an error for which the Segment Store thinks it is safer to stop writing to the log. A typical
message indicating this situation is as follows:
```
2022-11-03 06:14:07,826 4991892 [core-11] ERROR i.p.s.server.logs.DurableLog - DurableLog[18] Recovery FAILED.
io.pravega.segmentstore.storage.DataLogDisabledException: BookKeeperLog is disabled. Cannot initialize.
```

We can classify errors that can lead to disable a `DurableDataLog` into _persistent_ and _transient_.
Persistent errors are those that impact the data and/or metadata of the log (i.e., data corruption).
Recovering from these errors may require data recovery procedures as explained in other documents of this
section. Attempts to enable a log in this state are not advisable and will likely result in the Segment
Store disabling the log again, once the data corruption is detected. On the other hand, Transient errors 
are severe but recoverable errors that might also lead to disabling the log, such as an out of memory problem. 
This article focuses on the latter category and describes how to re-enable a `DurableDataLog` that has been 
_disabled_ for reasons different from data corruption.

# Repair Procedure

1. First, configure the [Pravega Admin CLI](https://github.com/pravega/pravega/blob/master/cli/admin/README.md)
from a location/instance that can access the Zookeeper and Bookkeeper services deployed in your cluster.

2. With the Pravega Admin CLI in place, run the following command: 
```
bk list
...
{
  "key": "log_summary",
  "value": {
    "logId": 1,
    "epoch": 81846,
    "version": 1062379,
    "enabled": false,
    "ledgers": 4,
    "truncation": "Sequence = 344181499234420, LedgerId = 3781242, EntryId = 2164"
  }
}
...
```
This command lists all the available Bookkeeper Logs in this cluster (please, be sure that the Pravega
Admin CLI has the `pravegaservice.container.count` parameter set to the same number of Segment Containers 
as in the cluster itself). The output of the above command shows that all the disabled Bookkeeper logs
exhibit `"enabled": false`.

3. Next, we need to be sure that we can recover the disabled Bookkeeper log(s) as a way to verify that
there are no data corruption issues. To this end, you need to run the following Pravega Admin CLI command
on all the disabled logs:
```
container recover [ID_OF_DISABLED_CONTAINER]
```

If the Segment Container recovers successfully, it means that there is no data corruption issue and it is
safe to enable the log again.

4. Finally, we have to enable the impacted Bookkeeper log(s) that are safe to enable again. To this end,
we need to type the following Pravega Admin CLI command on all the disabled logs:
```
bk enable [ID_OF_DISABLED_CONTAINER]
```

With this above command, the Segment Container associated to the re-enabled log should be able to resume
its operation. You can run again the `bk list` command to check that the current state of the Bookkeeper
logs is now _enabled_.

