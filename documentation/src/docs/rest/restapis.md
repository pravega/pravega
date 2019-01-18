# Pravega Controller APIs


<a name="overview"></a>
## Overview
List of admin REST APIs for the pravega controller service.


### Version information
*Version* : 0.0.1


### License information
*License* : Apache 2.0  
*License URL* : http://www.apache.org/licenses/LICENSE-2.0  
*Terms of service* : null


### URI scheme
*BasePath* : /v1  
*Schemes* : HTTP


### Tags

* ReaderGroups : Reader group related APIs
* Scopes : Scope related APIs
* Streams : Stream related APIs




<a name="paths"></a>
## Paths

<a name="createscope"></a>
### POST /scopes

#### Description
Create a new scope


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CreateScopeRequest**  <br>*required*|The scope configuration|[CreateScopeRequest](#createscope-createscoperequest)|

<a name="createscope-createscoperequest"></a>
**CreateScopeRequest**

|Name|Description|Schema|
|---|---|---|
|**scopeName**  <br>*optional*|**Example** : `"string"`|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**201**|Successfully created the scope|[ScopeProperty](#scopeproperty)|
|**409**|Scope with the given name already exists|No Content|
|**500**|Internal server error while creating a scope|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scopes


#### Example HTTP request

##### Request path
```
/scopes
```


##### Request body
```json
json :
{
  "scopeName" : "string"
}
```


#### Example HTTP response

##### Response 201
```json
json :
{
  "scopeName" : "string"
}
```


<a name="listscopes"></a>
### GET /scopes

#### Description
List all available scopes in pravega


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|List of currently available scopes|[ScopesList](#scopeslist)|
|**500**|Internal server error while fetching list of scopes|No Content|


#### Produces

* `application/json`


#### Tags

* Scopes


#### Example HTTP request

##### Request path
```
/scopes
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scopes" : [ {
    "scopeName" : "string"
  } ]
}
```


<a name="getscope"></a>
### GET /scopes/{scopeName}

#### Description
Retrieve details of an existing scope


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successfully retrieved the scope details|[ScopeProperty](#scopeproperty)|
|**404**|Scope with the given name not found|No Content|
|**500**|Internal server error while fetching scope details|No Content|


#### Produces

* `application/json`


#### Tags

* Scopes


#### Example HTTP request

##### Request path
```
/scopes/string
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scopeName" : "string"
}
```


<a name="deletescope"></a>
### DELETE /scopes/{scopeName}

#### Description
Delete a scope


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Successfully deleted the scope|No Content|
|**404**|Scope not found|No Content|
|**412**|Cannot delete scope since it has non-empty list of streams|No Content|
|**500**|Internal server error while deleting a scope|No Content|


#### Tags

* Scopes


#### Example HTTP request

##### Request path
```
/scopes/string
```


<a name="listreadergroups"></a>
### GET /scopes/{scopeName}/readergroups

#### Description
List reader groups within the given scope


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|List of all reader groups configured for the given scope|[ReaderGroupsList](#readergroupslist)|
|**404**|Scope not found|No Content|
|**500**|Internal server error while fetching the list of reader groups for the given scope|No Content|


#### Produces

* `application/json`


#### Tags

* ReaderGroups


#### Example HTTP request

##### Request path
```
/scopes/string/readergroups
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "readerGroups" : [ "object" ]
}
```


<a name="getreadergroup"></a>
### GET /scopes/{scopeName}/readergroups/{readerGroupName}

#### Description
Fetch the properties of an existing reader group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**readerGroupName**  <br>*required*|Reader group name|string|
|**Path**|**scopeName**  <br>*required*|Scope name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found reader group properties|[ReaderGroupProperty](#readergroupproperty)|
|**404**|Scope or reader group with given name not found|No Content|
|**500**|Internal server error while fetching reader group details|No Content|


#### Produces

* `application/json`


#### Tags

* ReaderGroups


#### Example HTTP request

##### Request path
```
/scopes/string/readergroups/string
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scopeName" : "string",
  "readerGroupName" : "string",
  "streamList" : [ "string" ],
  "onlineReaderIds" : [ "string" ]
}
```


<a name="createstream"></a>
### POST /scopes/{scopeName}/streams

#### Description
Create a new stream


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Body**|**CreateStreamRequest**  <br>*required*|The stream configuration|[CreateStreamRequest](#createstream-createstreamrequest)|

<a name="createstream-createstreamrequest"></a>
**CreateStreamRequest**

|Name|Description|Schema|
|---|---|---|
|**retentionPolicy**  <br>*optional*|**Example** : `"[retentionconfig](#retentionconfig)"`|[RetentionConfig](#retentionconfig)|
|**scalingPolicy**  <br>*optional*|**Example** : `"[scalingconfig](#scalingconfig)"`|[ScalingConfig](#scalingconfig)|
|**streamName**  <br>*optional*|**Example** : `"string"`|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**201**|Successfully created the stream with the given configuration|[StreamProperty](#streamproperty)|
|**404**|Scope not found|No Content|
|**409**|Stream with given name already exists|No Content|
|**500**|Internal server error while creating a stream|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams
```


##### Request body
```json
json :
{
  "streamName" : "string",
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0
  }
}
```


#### Example HTTP response

##### Response 201
```json
json :
{
  "scopeName" : "string",
  "streamName" : "string",
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0
  }
}
```


<a name="liststreams"></a>
### GET /scopes/{scopeName}/streams

#### Description
List streams within the given scope


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Query**|**showInternalStreams**  <br>*optional*|Optional flag whether to display system created streams. If not specified only user created streams will be returned|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|List of all streams configured for the given scope|[StreamsList](#streamslist)|
|**404**|Scope not found|No Content|
|**500**|Internal server error while fetching the list of streams for the given scope|No Content|


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams
```


##### Request query
```json
json :
{
  "showInternalStreams" : "string"
}
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "streams" : [ {
    "scopeName" : "string",
    "streamName" : "string",
    "scalingPolicy" : {
      "type" : "string",
      "targetRate" : 0,
      "scaleFactor" : 0,
      "minSegments" : 0
    },
    "retentionPolicy" : {
      "type" : "string",
      "value" : 0
    }
  } ]
}
```


<a name="getstream"></a>
### GET /scopes/{scopeName}/streams/{streamName}

#### Description
Fetch the properties of an existing stream


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Path**|**streamName**  <br>*required*|Stream name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Found stream properties|[StreamProperty](#streamproperty)|
|**404**|Scope or stream with given name not found|No Content|
|**500**|Internal server error while fetching stream details|No Content|


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams/string
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scopeName" : "string",
  "streamName" : "string",
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0
  }
}
```


<a name="updatestream"></a>
### PUT /scopes/{scopeName}/streams/{streamName}

#### Description
Update configuration of an existing stream


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Path**|**streamName**  <br>*required*|Stream name|string|
|**Body**|**UpdateStreamRequest**  <br>*required*|The new stream configuration|[UpdateStreamRequest](#updatestream-updatestreamrequest)|

<a name="updatestream-updatestreamrequest"></a>
**UpdateStreamRequest**

|Name|Description|Schema|
|---|---|---|
|**retentionPolicy**  <br>*optional*|**Example** : `"[retentionconfig](#retentionconfig)"`|[RetentionConfig](#retentionconfig)|
|**scalingPolicy**  <br>*optional*|**Example** : `"[scalingconfig](#scalingconfig)"`|[ScalingConfig](#scalingconfig)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successfully updated the stream configuration|[StreamProperty](#streamproperty)|
|**404**|Scope or stream with given name not found|No Content|
|**500**|Internal server error while updating the stream|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams/string
```


##### Request body
```json
json :
{
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0
  }
}
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scopeName" : "string",
  "streamName" : "string",
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0
  }
}
```


<a name="deletestream"></a>
### DELETE /scopes/{scopeName}/streams/{streamName}

#### Description
Delete a stream


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Path**|**streamName**  <br>*required*|Stream name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Successfully deleted the stream|No Content|
|**404**|Stream not found|No Content|
|**412**|Cannot delete stream since it is not sealed|No Content|
|**500**|Internal server error while deleting the stream|No Content|


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams/string
```


<a name="getscalingevents"></a>
### GET /scopes/{scopeName}/streams/{streamName}/scaling-events

#### Description
Get scaling events for a given datetime period.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Path**|**streamName**  <br>*required*|Stream name|string|
|**Query**|**from**  <br>*required*|Parameter to display scaling events from that particular datetime. Input should be milliseconds from Jan 1 1970.|integer (int64)|
|**Query**|**to**  <br>*required*|Parameter to display scaling events to that particular datetime. Input should be milliseconds from Jan 1 1970.|integer (int64)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successfully fetched list of scaling events.|[ScalingEventList](#scalingeventlist)|
|**404**|Scope/Stream not found.|No Content|
|**500**|Internal Server error while fetching scaling events.|No Content|


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams/string/scaling-events
```


##### Request query
```json
json :
{
  "from" : 0,
  "to" : 0
}
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "scalingEvents" : [ {
    "timestamp" : 0,
    "segmentList" : [ {
      "number" : 0,
      "startTime" : 0,
      "keyStart" : 0,
      "keyEnd" : 0
    } ]
  } ]
}
```


<a name="updatestreamstate"></a>
### PUT /scopes/{scopeName}/streams/{streamName}/state

#### Description
Updates the current state of the stream


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**scopeName**  <br>*required*|Scope name|string|
|**Path**|**streamName**  <br>*required*|Stream name|string|
|**Body**|**UpdateStreamStateRequest**  <br>*required*|The state info to be updated|[StreamState](#streamstate)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|Successfully updated the stream state|[StreamState](#streamstate)|
|**404**|Scope or stream with given name not found|No Content|
|**500**|Internal server error while updating the stream state|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Streams


#### Example HTTP request

##### Request path
```
/scopes/string/streams/string/state
```


##### Request body
```json
json :
{
  "streamState" : "string"
}
```


#### Example HTTP response

##### Response 200
```json
json :
{
  "streamState" : "string"
}
```




<a name="definitions"></a>
## Definitions

<a name="readergroupproperty"></a>
### ReaderGroupProperty

|Name|Description|Schema|
|---|---|---|
|**onlineReaderIds**  <br>*optional*|**Example** : `[ "string" ]`|< string > array|
|**readerGroupName**  <br>*optional*|**Example** : `"string"`|string|
|**scopeName**  <br>*optional*|**Example** : `"string"`|string|
|**streamList**  <br>*optional*|**Example** : `[ "string" ]`|< string > array|


<a name="readergroupslist"></a>
### ReaderGroupsList

|Name|Description|Schema|
|---|---|---|
|**readerGroups**  <br>*optional*|**Example** : `[ "object" ]`|< [readerGroups](#readergroupslist-readergroups) > array|

<a name="readergroupslist-readergroups"></a>
**readerGroups**

|Name|Description|Schema|
|---|---|---|
|**readerGroupName**  <br>*optional*|**Example** : `"string"`|string|


<a name="retentionconfig"></a>
### RetentionConfig

|Name|Description|Schema|
|---|---|---|
|**type**  <br>*optional*|**Example** : `"string"`|enum (LIMITED_DAYS, LIMITED_SIZE_MB)|
|**value**  <br>*optional*|**Example** : `0`|integer (int64)|


<a name="scalemetadata"></a>
### ScaleMetadata

|Name|Description|Schema|
|---|---|---|
|**segmentList**  <br>*optional*|**Example** : `[ "[segment](#segment)" ]`|< [Segment](#segment) > array|
|**timestamp**  <br>*optional*|**Example** : `0`|integer (int64)|


<a name="scalingconfig"></a>
### ScalingConfig

|Name|Description|Schema|
|---|---|---|
|**minSegments**  <br>*optional*|**Example** : `0`|integer (int32)|
|**scaleFactor**  <br>*optional*|**Example** : `0`|integer (int32)|
|**targetRate**  <br>*optional*|**Example** : `0`|integer (int32)|
|**type**  <br>*optional*|**Example** : `"string"`|enum (FIXED_NUM_SEGMENTS, BY_RATE_IN_KBYTES_PER_SEC, BY_RATE_IN_EVENTS_PER_SEC)|


<a name="scalingeventlist"></a>
### ScalingEventList

|Name|Description|Schema|
|---|---|---|
|**scalingEvents**  <br>*optional*|**Example** : `[ "[scalemetadata](#scalemetadata)" ]`|< [ScaleMetadata](#scalemetadata) > array|


<a name="scopeproperty"></a>
### ScopeProperty

|Name|Description|Schema|
|---|---|---|
|**scopeName**  <br>*optional*|**Example** : `"string"`|string|


<a name="scopeslist"></a>
### ScopesList

|Name|Description|Schema|
|---|---|---|
|**scopes**  <br>*optional*|**Example** : `[ "[scopeproperty](#scopeproperty)" ]`|< [ScopeProperty](#scopeproperty) > array|


<a name="segment"></a>
### Segment

|Name|Description|Schema|
|---|---|---|
|**keyEnd**  <br>*optional*|**Example** : `0`|integer (double)|
|**keyStart**  <br>*optional*|**Example** : `0`|integer (double)|
|**number**  <br>*optional*|**Example** : `0`|integer (int32)|
|**startTime**  <br>*optional*|**Example** : `0`|integer (int64)|


<a name="streamproperty"></a>
### StreamProperty

|Name|Description|Schema|
|---|---|---|
|**retentionPolicy**  <br>*optional*|**Example** : `"[retentionconfig](#retentionconfig)"`|[RetentionConfig](#retentionconfig)|
|**scalingPolicy**  <br>*optional*|**Example** : `"[scalingconfig](#scalingconfig)"`|[ScalingConfig](#scalingconfig)|
|**scopeName**  <br>*optional*|**Example** : `"string"`|string|
|**streamName**  <br>*optional*|**Example** : `"string"`|string|


<a name="streamstate"></a>
### StreamState

|Name|Description|Schema|
|---|---|---|
|**streamState**  <br>*optional*|**Example** : `"string"`|enum (SEALED)|


<a name="streamslist"></a>
### StreamsList

|Name|Description|Schema|
|---|---|---|
|**streams**  <br>*optional*|**Example** : `[ "[streamproperty](#streamproperty)" ]`|< [StreamProperty](#streamproperty) > array|





