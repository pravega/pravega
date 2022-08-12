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


# Metadata Table Segment Recovery


Please note that the steps outlined below are in continuation to the ones outlined as part of the recovery procedure described [here](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md)
That is we need to perform some of the steps mentioned in the above referenced doc, before continuing any of the steps present here.

The below outlined steps refer to recovery of Table Segments but only in cases of Metadata Segments which are a special case, as they need to 
be handled a little differently. Metadata Segments in Pravega are Table Segments, that hold metadata about all other "non-metadata" segments, which 
include the internal Segments as well as the User Segments. Metadata about these Metadata Segments in Pravega, are stored in Pravega Journals. More details
about the why and how part of Journals can be found in this PDP [here](https://github.com/pravega/pravega/wiki/PDP-34-(Simplified-Tier-2)#why-slts-needs-system-journal)
Pravega Journals is precisely what we would be attempting to update as part of the recovery procedure in this document.

It is precisely at this point here in the Table Segment Recovery procedure, that one would have to jump to the below steps in the case where we are
dealing with Metadata Segments.


# Detailed Steps
1) Once we determine that the table segment under repair is a Metadata Table Segment, we copy over the Attribute Index Chunks generated 
   in the output directory of [Step 4](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md#detailed-steps) to a separate directory of our choice.


2) Open the Admin CLI, and disable the Tier-1 of Container for which we would be doing the repair. To disable the Tier-1  
   run the below command:
   ```
       bk disable <containerId>
       Ex:-
          bk disable 3
   ```


3) Identify the latest journal file for the affected container. One can identify the latest journal file by simply listing the journal files 
   for the affected container. Before listing the journal files, go to the directory `\mnt\tier2\_system\containers` 
   where `\mnt\tier2` is the configured tier2 directory as mentioned in [Step2](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md#detailed-steps).
   Identify the latest journal file by doing a simple list for e.g: 
   ```
          ls -ltr | grep "container3"   
          for example could produce a listing like below:
             -rw-r--r-- 1 root root   321 Aug 12 02:23 _sysjournal.epoch1.container3.snapshot1
             -rw-r--r-- 1 root root  1114 Aug 12 02:25 _sysjournal.epoch1.container3.file2
             -rw-r--r-- 1 root root  1443 Aug 12 02:26 _sysjournal.epoch3.container3.file1

   ```
   We can see from the above listing that `_sysjournal.epoch3.container3.file1` is the latest journal file created.
   Copy over this file to a separate directory of your choice.


4) Identify the latest journal snapshot for the affected container. One can identify the latest journal snapshot by simply listing all the
   journal files for the affected container. Before listing make sure we are in the same directory `\mnt\tier2\_system\containers`
   and can run the below command. e.g:
   ```
         ls -ltr | grep container3 | grep snapshot
         for example could produce a listing like below:
             -rw-r--r-- 1 root root   321 Aug 12 02:23 _sysjournal.epoch1.container3.snapshot1
             -rw-r--r-- 1 root root   321 Aug 12 02:24 _sysjournal.epoch1.container3.snapshot2
             -rw-r--r-- 1 root root  1114 Aug 12 02:25 _sysjournal.epoch1.container3.file2
             -rw-r--r-- 1 root root  1443 Aug 12 02:26 _sysjournal.epoch3.container3.file1         

   ```
   We can see from the above listing that `_sysjournal.epoch1.container3.snapshot2` is the latest journal snapshot created.
   Copy over the latest snapshot identified above to a separate directory of your choice.


5) Open the Admin CLI again, and run the below command:
   ```
        update-latest-journal-snapshot <segment-chunk-path> <journal-file-path> <journal-snapshot-path> <output-directory>
        where:-
           segment-chunk-path: is the path containing the segment chunks created in Step 2 of Table Segment recovery.
           journal-file-path: is the path pointing to the journal file in the Step 3 above.
           journal-snapshot-path: is the path ponting to the latest snapshot file in Step 4 above.
           output-directory:  Path where one wants the latest modified snapshot file to be saved.
   ```


6) Copy over the snapshot file generated in the output directory of Step 5 above, back to its tier-2 path which is `\mnt\tier2\_system\containers`.


7) Copy over the segment chunks created in [Step 2](https://github.com/pravega/pravega/blob/master/documentation/src/docs/recovery-procedures/table-segment-recovery.md#detailed-steps) of the Table Segment recovery procedure to `\mnt\tier2\_system\containers`.


8) Delete all the journal files for the affected container except the newly copied snapshot file. 
   For example one can perform the delete like below:
   ```
        rm $(ls | grep container3 | grep -v snapshot)

   ```

   
8) Enable the Tier-1 of container again. One can run the below command:
   ```
       bk enable <containerId>
       Ex:
         bk enable 3
   ```

   
9) Restart segment store.
