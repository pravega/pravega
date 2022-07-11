# Pravega Controller APIs


<a name="overview"></a>
## Overview
List of admin REST APIs for the Pravega controller service.


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

* Health : Health check related APIs
* ReaderGroups : Reader group related APIs
* Scopes : Scope related APIs
* Streams : Stream related APIs




<a name="paths"></a>
## Paths

<a name="gethealth"></a>
### GET /health

#### Description
Return the Health of the Controller service.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The Health result of the Controller.|[HealthResult](#healthresult)|
|**500**|Internal server error while fetching the Health.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health
```


#### Example HTTP response

##### Response 200
```json
{
  "name" : "string",
  "status" : { },
  "readiness" : true,
  "liveness" : true,
  "details" : { },
  "children" : {
    "string" : "[healthresult](#healthresult)"
  }
}
```


<a name="getdetails"></a>
### GET /health/details

#### Description
Fetch the details of the Controller service.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The list of details.|[HealthDetails](#healthdetails)|
|**500**|Internal server error while fetching the health details of the Controller.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/details
```


#### Example HTTP response

##### Response 200
```json
{ }
```


<a name="getcontributordetails"></a>
### GET /health/details/{id}

#### Description
Fetch the details of a specific health contributor.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|The id of an existing health contributor.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The list of details for the health contributor with a given id.|[HealthDetails](#healthdetails)|
|**404**|The health details for the contributor with given id was not found.|No Content|
|**500**|Internal server error while fetching the health details for a given health contributor.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/details/string
```


#### Example HTTP response

##### Response 200
```json
{ }
```


<a name="getliveness"></a>
### GET /health/liveness

#### Description
Fetch the liveness state of the Controller service.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The alive status.|boolean|
|**500**|Internal server error while fetching the liveness state of the Controller.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/liveness
```


#### Example HTTP response

##### Response 200
```json
true
```


<a name="getcontributorliveness"></a>
### GET /health/liveness/{id}

#### Description
Fetch the liveness state of the specified health contributor.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|The id of an existing health contributor.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The alive status for the specified health contributor.|boolean|
|**404**|The liveness status for the contributor with given id was not found.|No Content|
|**500**|Internal server error while fetching the liveness state for a given health contributor.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/liveness/string
```


#### Example HTTP response

##### Response 200
```json
true
```


<a name="getreadiness"></a>
### GET /health/readiness

#### Description
Fetch the ready state of the Controller service.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The ready status.|boolean|
|**500**|Internal server error while fetching the ready state of the Controller.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/readiness
```


#### Example HTTP response

##### Response 200
```json
true
```


<a name="getcontributorreadiness"></a>
### GET /health/readiness/{id}

#### Description
Fetch the ready state of the health contributor.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|The id of an existing health contributor.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The readiness status for the health contributor with given id.|boolean|
|**404**|The readiness status for the contributor with given id was not found.|No Content|
|**500**|Internal server error while fetching the ready state for a given health contributor.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/readiness/string
```


#### Example HTTP response

##### Response 200
```json
true
```


<a name="getstatus"></a>
### GET /health/status

#### Description
Fetch the status of the Controller service.


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The health status of the Controller.|[HealthStatus](#healthstatus)|
|**500**|Internal server error while fetching the health status of the Controller.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/status
```


#### Example HTTP response

##### Response 200
```json
{ }
```


<a name="getcontributorstatus"></a>
### GET /health/status/{id}

#### Description
Fetch the status of a specific health contributor.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|The id of an existing health contributor.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The health status of the Controller.|[HealthStatus](#healthstatus)|
|**404**|The health status for the contributor with given id was not found.|No Content|
|**500**|Internal server error while fetching the health status of a given health contributor.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/status/string
```


#### Example HTTP response

##### Response 200
```json
{ }
```


<a name="getcontributorhealth"></a>
### GET /health/{id}

#### Description
Return the Health of a health contributor with a given id.


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|The id of an existing health contributor.|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|The Health result of the Controller.|[HealthResult](#healthresult)|
|**404**|A health provider for the given id could not be found.|No Content|
|**500**|Internal server error while fetching the health for a given contributor.|No Content|


#### Produces

* `application/json`


#### Tags

* Health


#### Example HTTP request

##### Request path
```
/health/string
```


#### Example HTTP response

##### Response 200
```json
{
  "name" : "string",
  "status" : { },
  "readiness" : true,
  "liveness" : true,
  "details" : { },
  "children" : {
    "string" : "[healthresult](#healthresult)"
  }
}
```


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
{
  "scopeName" : "string"
}
```


#### Example HTTP response

##### Response 201
```json
{
  "scopeName" : "string"
}
```


<a name="listscopes"></a>
### GET /scopes

#### Description
List all available scopes in Pravega


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
{
  "scopeName" : "string",
  "readerGroupName" : "string",
  "streamList" : [ "string" ],
  "onlineReaderIds" : [ "string" ]
}
```


<a name="deletereadergroup"></a>
### DELETE /scopes/{scopeName}/readergroups/{readerGroupName}

#### Description
Delete a reader group


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**readerGroupName**  <br>*required*|Reader group name|string|
|**Path**|**scopeName**  <br>*required*|Scope name|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**204**|Successfully deleted the reader group|No Content|
|**404**|Reader group with given name not found|No Content|
|**500**|Internal server error while deleting a reader group|No Content|


#### Tags

* ReaderGroups


#### Example HTTP request

##### Request path
```
/scopes/string/readergroups/string
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
|**rolloverSizeBytes**  <br>*optional*|**Example** : `"[rolloversizebytes](#rolloversizebytes)"`|[RolloverSizeBytes](#rolloversizebytes)|
|**scalingPolicy**  <br>*optional*|**Example** : `"[scalingconfig](#scalingconfig)"`|[ScalingConfig](#scalingconfig)|
|**streamName**  <br>*optional*|**Example** : `"string"`|string|
|**streamTags**  <br>*optional*|**Example** : `"[tagslist](#tagslist)"`|[TagsList](#tagslist)|
|**timestampAggregationTimeout**  <br>*optional*|**Example** : `"[timestampaggregationtimeout](#timestampaggregationtimeout)"`|[TimestampAggregationTimeout](#timestampaggregationtimeout)|


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
    "value" : 0,
    "timeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    },
    "maxValue" : 0,
    "maxTimeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    }
  },
  "streamTags" : { },
  "timestampAggregationTimeout" : { },
  "rolloverSizeBytes" : { }
}
```


#### Example HTTP response

##### Response 201
```json
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
    "value" : 0,
    "timeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    },
    "maxValue" : 0,
    "maxTimeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    }
  },
  "tags" : { }
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
|**Query**|**filter_type**  <br>*optional*|Filter options|enum (showInternalStreams, tag)|
|**Query**|**filter_value**  <br>*optional*|value to be passed. must match the type passed with it.|string|


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


#### Example HTTP response

##### Response 200
```json
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
      "value" : 0,
      "timeBasedRetention" : {
        "days" : 0,
        "hours" : 0,
        "minutes" : 0
      },
      "maxValue" : 0,
      "maxTimeBasedRetention" : {
        "days" : 0,
        "hours" : 0,
        "minutes" : 0
      }
    },
    "tags" : { }
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
    "value" : 0,
    "timeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    },
    "maxValue" : 0,
    "maxTimeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    }
  },
  "tags" : { }
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
|**rolloverSizeBytes**  <br>*optional*|**Example** : `"[rolloversizebytes](#rolloversizebytes)"`|[RolloverSizeBytes](#rolloversizebytes)|
|**scalingPolicy**  <br>*optional*|**Example** : `"[scalingconfig](#scalingconfig)"`|[ScalingConfig](#scalingconfig)|
|**streamTags**  <br>*optional*|**Example** : `"[tagslist](#tagslist)"`|[TagsList](#tagslist)|
|**timestampAggregationTimeout**  <br>*optional*|**Example** : `"[timestampaggregationtimeout](#timestampaggregationtimeout)"`|[TimestampAggregationTimeout](#timestampaggregationtimeout)|


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
{
  "scalingPolicy" : {
    "type" : "string",
    "targetRate" : 0,
    "scaleFactor" : 0,
    "minSegments" : 0
  },
  "retentionPolicy" : {
    "type" : "string",
    "value" : 0,
    "timeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    },
    "maxValue" : 0,
    "maxTimeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    }
  },
  "streamTags" : { },
  "timestampAggregationTimeout" : { },
  "rolloverSizeBytes" : { }
}
```


#### Example HTTP response

##### Response 200
```json
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
    "value" : 0,
    "timeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    },
    "maxValue" : 0,
    "maxTimeBasedRetention" : {
      "days" : 0,
      "hours" : 0,
      "minutes" : 0
    }
  },
  "tags" : { }
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
/scopes/string/streams/string/scaling-events?from=0&to=0
```


#### Example HTTP response

##### Response 200
```json
{
  "scalingEvents" : [ {
    "timestamp" : 0,
    "segmentList" : [ {
      "number" : 0,
      "startTime" : 0,
      "keyStart" : 0,
      "keyEnd" : 0
    } ],
    "splits" : 0,
    "merges" : 0
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
{
  "streamState" : "string"
}
```


#### Example HTTP response

##### Response 200
```json
{
  "streamState" : "string"
}
```




<a name="definitions"></a>
## Definitions

<a name="healthdetails"></a>
### HealthDetails
*Type* : < string, string > map


<a name="healthresult"></a>
### HealthResult

|Name|Description|Schema|
|---|---|---|
|**children**  <br>*optional*|**Example** : `{<br>  "string" : "[healthresult](#healthresult)"<br>}`|< string, [HealthResult](#healthresult) > map|
|**details**  <br>*optional*|**Example** : `"[healthdetails](#healthdetails)"`|[HealthDetails](#healthdetails)|
|**liveness**  <br>*optional*|**Example** : `true`|boolean|
|**name**  <br>*optional*|**Example** : `"string"`|string|
|**readiness**  <br>*optional*|**Example** : `true`|boolean|
|**status**  <br>*optional*|**Example** : `"[healthstatus](#healthstatus)"`|[HealthStatus](#healthstatus)|


<a name="healthstatus"></a>
### HealthStatus
*Type* : enum (UP, STARTING, NEW, UNKNOWN, FAILED, DOWN)


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
|**maxTimeBasedRetention**  <br>*optional*|**Example** : `"[timebasedretention](#timebasedretention)"`|[TimeBasedRetention](#timebasedretention)|
|**maxValue**  <br>*optional*|**Example** : `0`|integer (int64)|
|**timeBasedRetention**  <br>*optional*|**Example** : `"[timebasedretention](#timebasedretention)"`|[TimeBasedRetention](#timebasedretention)|
|**type**  <br>*optional*|Indicates if retention is by space or time.  <br>**Example** : `"string"`|enum (LIMITED_DAYS, LIMITED_SIZE_MB)|
|**value**  <br>*optional*|**Example** : `0`|integer (int64)|


<a name="rolloversizebytes"></a>
### RolloverSizeBytes
*Type* : long


<a name="scalemetadata"></a>
### ScaleMetadata

|Name|Description|Schema|
|---|---|---|
|**merges**  <br>*optional*|**Example** : `0`|integer (int64)|
|**segmentList**  <br>*optional*|**Example** : `[ "[segment](#segment)" ]`|< [Segment](#segment) > array|
|**splits**  <br>*optional*|**Example** : `0`|integer (int64)|
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
|**tags**  <br>*optional*|**Example** : `"[tagslist](#tagslist)"`|[TagsList](#tagslist)|


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


<a name="tagslist"></a>
### TagsList
*Type* : < string > array


<a name="timebasedretention"></a>
### TimeBasedRetention

|Name|Description|Schema|
|---|---|---|
|**days**  <br>*optional*|**Example** : `0`|integer (int64)|
|**hours**  <br>*optional*|**Example** : `0`|integer (int64)|
|**minutes**  <br>*optional*|**Example** : `0`|integer (int64)|


<a name="timestampaggregationtimeout"></a>
### TimestampAggregationTimeout
*Type* : long





