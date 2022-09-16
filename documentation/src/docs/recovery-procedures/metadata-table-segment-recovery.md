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


# Metadata Table Segment Attribute Index Recovery


This document refers to recovery of Attribute Index Table Segments but only in cases of Metadata Segments which are a special case, as they need to
be handled a little differently. Metadata Segments in Pravega are Table Segments that hold metadata about all other "non-metadata" Segments, which
include the internal Segments as well as the User Segments. Metadata about these Metadata Segments themselves, are stored in Pravega System Journals. More details
about the why and how part of Journals can be found in this PDP [here](https://github.com/pravega/pravega/wiki/PDP-34-(Simplified-Tier-2)#why-slts-needs-system-journal).
Pravega System Journals, is concretely what we would be attempting to update as part of the recovery procedure in this document.


Please note that the steps outlined below are in continuation to the ones outlined as part of the recovery procedure described [here](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md).
That is, we need to perform some of the steps mentioned in the above referenced doc, before continuing any of the steps present here.


It is precisely at this point [here](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md#important) in 
the Table Segment Recovery procedure, that one would have to jump to the below steps in the case of dealing with Metadata Segments.


# Detailed Steps
1) Once we determine that the Table Segment under repair is a Metadata Table Segment, we copy over the Attribute Index Chunks generated 
   in the output directory of [Step 4](https://github.com/pravega/pravega/blob/a5088a464275d5ea90adb09ac39027332e87a8e3/documentation/src/docs/recovery-procedures/table-segment-recovery.md?plain=1#L129) to a separate directory of our choice.


2) Open the Admin CLI, and disable Tier-1 for the Container owning the Metadata Segment. The owning Container ID can be figured out from the Metadata Segment Chunk 
   name. For example, if one of the Metadata Chunk file is say `metadata_2.E-1-O-0.88990c40-58a3-463e-8936-652d89fa95ba` then the container is 2 from `metadata_2` in file name.
   To disable the Tier-1, run the below command:
   ```
       bk disable <containerId>
       Ex:-
          bk disable 2
   ```


3) Identify the latest Journal file for the affected Container. One can identify the latest Journal file by simply listing the Journal files for the affected Container. 
   Before listing the Journal files, go to the directory `/mnt/tier2/_system/containers` where `/mnt/tier2` is the configured Tier2 directory 
   as mentioned in [Step2](https://github.com/pravega/pravega/blob/a5088a464275d5ea90adb09ac39027332e87a8e3/documentation/src/docs/recovery-procedures/table-segment-recovery.md?plain=1#L70).
   Identify the latest Journal file by doing a simple `ls` for e.g: 
   ```
          ls -ltr | grep "container3"   
          For example, could produce a listing like below:
             -rw-r--r-- 1 root root   321 Aug 12 02:23 _sysjournal.epoch1.container3.snapshot1
             -rw-r--r-- 1 root root  1114 Aug 12 02:25 _sysjournal.epoch1.container3.file2
             -rw-r--r-- 1 root root  1443 Aug 12 02:26 _sysjournal.epoch3.container3.file1

   ```
   We can see from the above listing that `_sysjournal.epoch3.container3.file1` is the latest Journal file created.
   Copy over this file to a separate directory of your choice.


4) Identify the latest Journal Snapshot for the affected Container. One can identify the latest Journal Snapshot by simply listing all the
   Journal files for the affected container. Before listing make sure we are in the same directory `/mnt/tier2/_system/containers`
   and can run the below command. e.g:
   ```
         ls -ltr | grep container3 | grep snapshot
         For example, could produce a listing like below:
             -rw-r--r-- 1 root root   321 Aug 12 02:23 _sysjournal.epoch1.container3.snapshot1
             -rw-r--r-- 1 root root   321 Aug 12 02:24 _sysjournal.epoch1.container3.snapshot2
             -rw-r--r-- 1 root root  1114 Aug 12 02:25 _sysjournal.epoch1.container3.file2
             -rw-r--r-- 1 root root  1443 Aug 12 02:26 _sysjournal.epoch3.container3.file1         

   ```
   We can see from the above listing that `_sysjournal.epoch1.container3.snapshot2` is the latest Journal Snapshot created.
   Copy over the latest Snapshot identified above to a separate directory of your choice.


5) Open the Admin CLI again, and run the below command:
   ```
        update-latest-journal-snapshot <segment-chunk-path> <journal-file-path> <journal-snapshot-path> <output-directory>
        where:-
           segment-chunk-path: is the path containing the Segment Chunks created in Step 2 of Table Segment recovery.
           journal-file-path: is the path pointing to the journal file in the Step 3 above.
           journal-snapshot-path: is the path ponting to the latest snapshot file in Step 4 above.
           output-directory: Path where one wants the latest modified Snapshot file to be saved.
   ```
    What the above command does is list the corrected/fixed Segment chunks pointed to by the path `segment-chunk-path`, sort them based on their epoch
    and offsets. And then reads them in the sorted order, and builds metadata about these chunks from their names and length. Once this metadata is 
    generated, it updates this metadata for the Segment in question, and generates a Pravega System Journal Snapshot with this updated metadata.


6) Copy over the Snapshot file generated in the output directory of Step 5 above, back to its Tier-2 path which is `/mnt/tier2/_system/containers`.


7) Copy over the Segment Chunks created in [Step 4](https://github.com/pravega/pravega/blob/a5088a464275d5ea90adb09ac39027332e87a8e3/documentation/src/docs/recovery-procedures/table-segment-recovery.md?plain=1#L129) of the Table Segment recovery procedure to `/mnt/tier2/_system/containers`.


8) Remove all the Journal files for the affected Container except the newly copied Snapshot file. To do so make sure you are in the same Tier-2 path
   which is `/mnt/tier2/_system/containers`.
   For example one can perform the removal like below:
   ```
        rm $(ls | grep container3 | grep -v snapshot)

   ```

   
8) Enable the Tier-1 of Container again. One can run the below command:
   ```
       bk enable <containerId>
       Ex:
         bk enable 2
   ```

   
9) Restart Segment Store.
