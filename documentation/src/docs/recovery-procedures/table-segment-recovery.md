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
* [Possible Root Cause](#root-cause)
* [Repair Procedure](#repair-procedure)


# Issue
One of the issues recently seen in Pravega was some kind of data corruption happening on Attribute Index Segment. The issue manifested with the below error message popping up in logs:-
```
ERROR i.p.s.s.h.h.PravegaRequestProcessor - [requestId=2205414166888448] Error (Segment = '_system/_tables/completedTransactionsBatch-0', Operation = 'updateTableEntries')
io.pravega.segmentstore.server.DataCorruptionException: BTreeIndex operation failed. Index corrupted.
...
Caused by: io.pravega.common.util.IllegalDataFormatException: [AttributeIndex[3-423]] Wrong footer information. RootPage Offset (3842188288) + Length (142858) exceeds Footer Offset (15007837).
```
The original issue filed can be found here:- https://github.com/pravega/pravega/issues/6712

The Attribute Index Segment is basically a BTree Index of the data (key/value pair) that goes in its associated main Table Segment. It is internally a B+Tree (existing on Pravega Segment), and is organized as as set of Pages written to storage (https://cncf.pravega.io/blog/2019/11/21/segment-attributes). Each Page can be an index Page or leaf Page (holding data entries) having tree nodes. Each write of Pages to Storage ends with a footer, which is a pointer to the latest location of the root node in the tree. The above error indicates that the footer for the last write of Pages to Storage has been corrupted, as it contains inconsistent information. The issue having happened so, indicates some kind of corruption in the underlying Attribute Index Segment.


# Possible Root Causes

  We do not as yet know the exact reason behind what could have caused such a data corruption, which is why this recovery procedure exists today but we suspect below possible reasons:-

  -  Writing a malformed footer/Page to Storage:
     The above issue has been seen in conjuntion with a CacheFullException. That is we see a cacheFullException first and couple of minutes later is when we see the DataCorruptionException described above.That is the BTree index      pages accessed are also cached in Pravega's internal cache. And trying to access these pages when the cache is full could have led to some wrong data being read at some arbritrary offsets, inturn leading to malformed footers     being written to storage.

  -  Corruption at the Tier-2 storage level:
     It could also be an issue pertaining to the underlying Tier-2 storage that Pravega uses. That is some kind of data corruption or loss  having occured in the storage system that Pravega uses that could have caused such an exc     eption.


# Repair Procedure

The repair procedure aims at recovering Pravega from such a situation. Here is what the recovery procedure aims to do. Recall that the corruption that we see occurs at the Attribute Index Segment level, which is just an index built by Pravega in the background of the data that exists in the associated primary Table Segment. This index helps associate "keys" with "offsets" in the primary Table Segment where "values" actually reside. The main assumption ofthis recovery procedure is this primary Table Segment is perfectly fine and the corruption happens only at the index level.

In a nutshell, the recovery procedure of a Table Segment Attribute Index involves re-reading the key/value pairs in the primary Table Segment and feeding them to a local in-process Pravega cluster. Doing so would result in an Table Segment Attribute Index generated in the storage directory of that local in-process Pravega cluster. To do so, we need to collect, for the corrupted Table Segment Attribute Index, all the primary chunks from the Pravega cluster to repair. Then, we use the Admin CLI `dataRecovery tableSegment-recovery` command (see #6753) to re-create locally the new Table Segment Attribute Index. Finally, we need to replace the newly generated Table Segment Attribute Index by the corrupted one in the Pravega cluster to repair.

In the next section we look at the detailed set of steps about carrying out the procedure. Also please note that the below described procedure assumes the use of "File System" as Tier-2 storage. Other storage interfaces used in place of "File System" as Tier-2 storage, would only differ in the way we would access the resources (like objects in case of Dell EMC ECS) and not the actual steps.

# Detailed Steps

1) From the error, first determine the Table Segment name:-
    ```
      Caused by: io.pravega.common.util.IllegalDataFormatException: [AttributeIndex[3-423]] Wrong footer information. RootPage Offset (3842188288) + Length (142858) exceeds Footer Offset (15007837).
    ```
    Here `AttributeIndex[3-423]` indicates Container 3 and Segment id 423.

    Find out from the logs what is the name of the Table Segment with id 423. To do so one can grep "MapStreamSegmentId" and pick the one, for the Table Segment id in question.

2) Go to the Tier-2 directory having the Segment chunks. Usually the root of this directory is `/mnt/tier-2`. And the Table Segment Chunks can be found under `/mnt/tier2/_system/_tables`.

3) Copy over the main Table Segment Chunks to a directory of your choice.

    Here is an example of a listing of the said directoty.

    ```
	-drwxrwxr-x 12 osboxes osboxes    4096 Jun  2 11:45  ..
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

    Lets say the affected Table Segment name that you find out from step 1 is "completedTransactionsBatch-0". One would copy over the chunks of this Segment (note that they are main Table Segment chunks and they do not have "attr    ibutes.index" string in their name). So one would copy over `completedTransactionsBatch-0.E-1-O-0.b29fcb2f-e71f-4971-bc43-5c0a801c35e7`, 
    `completedTransactionsBatch-0.E-2-O-331331.71afe323-ae99-4f3d-a1f0-af4db475fef9` and `completedTransactionsBatch-0.E-5-O-1318135.8c0d3e40-bea7-4fbe-96ca-ac76c80283ad` to a directory of your choice.

4) Start the Pravega Admin Cli (assuming its configured correctly to run) and enter the below command.

    ```
      data-recovery tableSegment-recovery <directory_where_you_copied to in step 3> <Table Segment name> <directory where you want to copy the output chunks to>

      Ex:-
        data-recovery tableSegment-recovery /foo/bar completedTransactionsBatch-0 /bar/foo/bar
    ```

5) Copy these generated Table Segment Attribute Index chunks back to the Tier-2 directory we identified in step 2.

6) Edit the metadata of the Segments to reflect these chunks.

    a)  For example, lets say the chunk generated by the command in the folder is :-
 
               13003178 Jun  3 02:55 completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b

    b)  Copy this chunk to the Tier-2 directory i.e `/mnt/tier2/_system/_tables/`


    c)  Remove the older set of chunks. That is from the above "ls" listed files above, one would remove:-

           -rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101208573.10e6a6e8-8665-4e57-bfe4-085eee5cf46b'
           -rw-r--r--  1 osboxes osboxes  128767 Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-101337340.a2e5cf79-914e-4a01-9c35-b390a6f61f4a'
           -rw-r--r--  1 osboxes osboxes  19876  Jun  3 04:41 'completedTransactionsBatch-0$attributes.index.E-5-O-102496243.b032e528-589b-421b-8977-178d72128dcf'
          

    d) Perform a get on the Attribute Index Segment in the CLI:

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

              
                - Increment the version.
                - Update the length to reflect the cumulative length of chunk(s). In the case above we would update it to 13003178.
                - Update the chunk count. in the case above it is 1, since there is just one chunk generated in step 5a.
                - Update the startOffset to 0.
                - Update the firstChunk to _system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b
                - Update the lastChunk to the same as there is only one chunk. 
                - Update the firstChunkStartOffset to 0. Derived from "E-1-[O]-0" part in file name.
                - Update the lastChunkStartOffset to 0. (first and last chunks are same.)
              


     f) We have updated the Attribute Index Segment metadata with the chunk files and their other attributes. Next step is to create the Chunk Metadata itself.

            Perform a put for the Attribute Index chunk we have in 6a.
            
            This is how the put/create command would look:-
               
              
                table-segment put _system/_containers/storage_metadata_3 _system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b key=_system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b;version=1654199182158;metadataType=ChunkMetadata;name=_system/_tables/completedTransactionsBatch-0$attributes.index.E-1-O-0.09cb6598-da98-43ad-8d9c-44ca6d523c4b;length=13003178;nextChunk=null;status=1
              

             One would enter the following fields to create the metadata:-

                --key=chunk file name
                --version= a random long number can be put in
                --name = chunk file name
                --length = length of the chunk file
                --nextChunk = null (as there is only one chunk)
             
             Note: In case of multiple chunks, one would have to create a "put"  for each chunk after identifying the sequence of chunks based on the offsets in their name.



7) Restart Segment Store. Upon restarting one should not see a "DataCorruptionException" in the Segment Store logs. One can even perform a Table Segment `get-info` command and check if the number of keys increase to see if the writes after ther repair are going through.





