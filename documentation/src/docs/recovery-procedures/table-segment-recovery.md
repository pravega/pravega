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

# Table Segment Recovery

* [Issue](#issue)
* [Root Cause](#root-cause)
* [Repair Procedure](#repair-procedure)


The following section describes repair procedure for repairing a corrupted table segment in Pravega. 

# Issue
One of the issues recently seen in Pravega was some data corruption happening on Attribute index segment. The issue manifested with the below error message popping up in logs:-
```
ERROR i.p.s.s.h.h.PravegaRequestProcessor - [requestId=2205414166888448] Error (Segment = '_system/_tables/completedTransactionsBatch-0', Operation = 'updateTableEntries')
io.pravega.segmentstore.server.DataCorruptionException: BTreeIndex operation failed. Index corrupted.
...
Caused by: io.pravega.common.util.IllegalDataFormatException: [AttributeIndex[3-423]] Wrong footer information. RootPage Offset (3842188288) + Length (142858) exceeds Footer Offset (15007837).
```

The attribute index segment is basically a BTree Index of the data(key/value pair) that goes in its associated main Table Segment. It is internally a B+Tree (existing on Pravega segment), and is organized as as set of pages written to storage. Each page can be an index page or leaf page (holding data entries) having tree nodes. After writing a collection of pages we write something called as a footer that basically points to latest location of the root. The error above basically indicates the fact that  since footer exists at the highest offsets of every write, the root page's length from its offset cannot exceed that of footer's. The issue having happened so, indicates some kind of corruption in the underlying attribute index segment.


# Root Cause

The above issue has been seen in conjuntion with a CacheFullException. That is we see a cacheFullException first and couple of minutes later is when we see the DataCorruptionException described above. Although we have not been able to do a exact root cause analysis on why such a corruption occured we suspect it could be linked to us getting
a cacheFullException. That is the BTree index pages accessed are also cached in Pravega's internal cache. And trying to access these pages when the cache is full could have led to some wrong data being read, and eventually wrong data being written to the underlying Pravega segments. 
It could also be an issue pertaining to the underlying tier-2 storage that Pravega uses. That is some kind of corruption having occured in the storage system that Pravega uses that could have caused such an exception.
However we do not know the exact root cause of what might have led to this and that is the reason this Recovery procudure exists.



# Repair Procedure

The repair procedure aims at recovering Pravega from such a situation. Here is what the recovery procedure aims to do. Recall that the corruption that we see occurs in the attribute index segment which is just an index of the data that exists in the associated primary table segment( holding key-value pairs ) and that this primary table segment is perfectly fine. So what if we re-read this main table segment and generate an index again which is clean. Thats what the repair procedure does. It revolves around re-reading 
the key values pairs in the main table segment, feeding them to a in-process Pravega. Doing so would result in an attribute index segment generated into the storage directory that this in-process Pravega points to. 

We replace this generated clean attribute index segment back to the Pravega cluster which had the datacoruption, therby freeing it from the issue.


# Detailed Steps

1) From the error, first determine the segment name:-
```
Caused by: io.pravega.common.util.IllegalDataFormatException: [AttributeIndex[3-423]] Wrong footer information. RootPage Offset (3842188288) + Length (142858) exceeds Footer Offset (15007837).
```
Here AttributeIndex[3-423] indicates container 3 and segment id 423.

Find out from the logs what is the name of the segment with id 423. To do so one can grep "MapstreamSegmentId" and pick the one for the segment id in question.

2) Change to the tier-2 directory having the segment chunks. Usually the root of this directory is /mnt/tier-2. And the table segment chunks can be found under `/mnt/tier2/_system/_tables`.

3) Copy over the main table segment chunks to a directory of your choice.

  Here is an example of a listing of the said directoty.

```
drwxrwxr-x 12 osboxes osboxes    4096 Jun  2 11:45  ..
-rw-r--r--  1 osboxes osboxes      77 Jun  2 11:46  scopes.E-1-O-0.0708cef5-1dd4-4b60-9a95-3db4319f1024
-rw-r--r--  1 osboxes osboxes     124 Jun  2 11:46 'scopes$attributes.index.E-1-O-0.2cb32bde-bd7e-4419-b6e8-9ddfe2519bfe'
drwxrwxr-x  4 osboxes osboxes    4096 Jun  2 11:46  test
-rw-r--r--  1 osboxes osboxes      18 Jun  2 11:46  completedTransactionsBatches.E-1-O-0.8ee9c66c-dc15-4f7a-bac8-beeeff916233
-rw-r--r--  1 osboxes osboxes      50 Jun  2 11:46 'completedTransactionsBatches$attributes.index.E-1-O-0.08af4870-c96b-4d66-a08e-c8cbed51d26f'
-rw-r--r--  1 osboxes osboxes  331331 Jun  2 14:02  completedTransactionsBatch-0.E-1-O-0.b29fcb2f-e71f-4971-bc43-5c0a801c35e7
-rw-r--r--  1 osboxes osboxes  986804 Jun  3 01:49  completedTransactionsBatch-0.E-2-O-331331.71afe323-ae99-4f3d-a1f0-af4db475fef9
-rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101208573.10e6a6e8-8665-4e57-bfe4-085eee5cf46b'
-rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101337340.a2e5cf79-914e-4a01-9c35-b390a6f61f4a'
-rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-102496243.b032e528-589b-421b-8977-178d72128dcf'
-rw-r--r--  1 osboxes osboxes 2241876 Jun  3 04:41  completedTransactionsBatch-0.E-5-O-1318135.8c0d3e40-bea7-4fbe-96ca-ac76c80283ad
```

Lets say the affected table segment name that you find out from step 1 is "completedTransactionsBatch-0". One would copy over the chunks of this segment( note that they are main table segment chunks and they do not have "attributes.index" string in their name). So one would copy over completedTransactionsBatch-0.E-1-O-0.b29fcb2f-e71f-4971-bc43-5c0a801c35e7, 
completedTransactionsBatch-0.E-2-O-331331.71afe323-ae99-4f3d-a1f0-af4db475fef9 and completedTransactionsBatch-0.E-5-O-1318135.8c0d3e40-bea7-4fbe-96ca-ac76c80283ad to a directory of your choice.

4) Fire up the admin-cli (assuming its configured correctly to run) and enter the below command.

```
data-recovery tableSegment-recovery <directory_where_you_copied to in step 3> <table segment name> <directory where you want to copy the output chunks to>

Ex:-
data-recovery tableSegment-recovery /foo/bar completedTransactionsBatch-0 /bar/foo/bar
```

5) Copy these generated attribute chunks back to the tier-2 directory we identified in step 2.

6) Edit the metadata of the segments to reflect these chunks.

    a)  For example  Lets say the chunk generated by the command in the folder is :-
 
                13003178 Jun  3 02:55 completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b

    b)  Copy this chunk to the tier-2 directory i.e /mnt/tier2/_system/_tables/


    c)  Remove the older set of chunks. That is from the above "ls" listed files above, one would remove:-

           -rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101208573.10e6a6e8-8665-4e57-bfe4-085eee5cf46b'
	   -rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101337340.a2e5cf79-914e-4a01-9c35-b390a6f61f4a'
	   -rw-r--r--  1 osboxes osboxes  19876  Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-102496243.b032e528-589b-421b-8977-178d72128dcf'
          

    d) Perform a get on the attribute index segment in the CLI

       table-segment get _system/containers/storage_metadata_<owning Container ID here> <segmentName>$attributes.index <SS_Pod_IP_OWNING_THE_CONTAINER>


    e) Lets say one gets the below details by performing "get" above.

       
          > table-segment get _system/containers/storage_metadata_3 _system/_tables/completedTransactionsBatch-0$attributes.index 127.0.1.1
		For the given key: _system/_tables/completedTransactionsBatch-0$attributes.index
		SLTS metadata info: 
		key = _system/_tables/completedTransactionsBatch-0$attributes.index;
		version = 1654241463251;
		metadataType = SegmentMetadata;
		name = _system/_tables/completedTransactionsBatch-0$attributes.index;
		length = 13488706;
		chunkCount = 3;
		startOffset = 13135328;
		status = 1;
		maxRollingLength = 128767;
		firstChunk = _system/_tables/completedTransactionsBatch-0$attributes.index.E-5-O-13131945.29bd2254-2e77-4338-a41a-f9c2af07b913;
		lastChunk = _system/_tables/completedTransactionsBatch-0$attributes.index.E-5-O-13389479.5779f8d5-89ad-4bc6-87fe-3f99480a7271;
		lastModified = 0;
		firstChunkStartOffset = 13131945;
		lastChunkStartOffset = 13389479;
		ownerEpoch = 5;

       
        One would edit the following fields to update the metadata to reflect the chunk properties we have in step 5a).

              
                - Increment the version
                - update the length to reflect the cumulative length of chunk(s). In the case above we would update it to 13003178.
                - update the chunk count. in the case above it is 1, since there is just one chunk generated in step 5a.
                - update the startOffset to 0.
                - update the firstChunk to _system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b
                - update the lastChunk to the same as there is only one chunk. 
                - update the firstChunkStartOffset to 0. Derived from "E-1-[O]-0" part in file name.
                - update the lastChunkStartOffset to 0. (first and last chunks are same.)
              


     f) We have updated the attribute index segment metadata with the chunk files and their other attributes. Next step is to create the chunkMetadata itself.

            Perform a put for the attribute index chunk we have in 6a.
            
            This is how the put/create command would look:-
               
              
                table-segment put _system/_containers/storage_metadata_3 _system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b key=_system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b;version=1654199182158;metadataType=ChunkMetadata;name=_system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b;length=13003178;nextChunk=null;status=1
              

             One would enter the following fields to create the metadata:-

		-- key=chunk file name
		--version= a random long number can be put in
		--name = chunk file name
		--length = length of the chunk file
		--nextChunk = null (as there is only one chunk)
             
             Note: In case of multiple chunks, one would have to create a "put"  for each chunk after identifying the sequence of chunks based on the offsets in their name.



7) Restart SegmentStore. Upon restarting one should not basically see a "DataCorruptionException" in the segment store logs. One can even perform a table-segment get-info command and check if the number of keys increase to see if   the writes after ther repair are going through.





