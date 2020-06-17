#!/bin/bash
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

curl -i -k -u $1:$2 "http://$3:9091/v1/scopes/" -X POST -d '{"scopeName":"project58"}' -H 'Content-Type: application/json' 
curl -i -k -u $1:$2 "http://$3:9091/v1/scopes/project58/streams" -X POST -d '{"scopeName":"project58","streamName":"logstream2","scalingPolicy":{"type":"FIXED_NUM_SEGMENTS","minSegments":1},"retentionPolicy":{"type":"LIMITED_DAYS","value":1}}' -H 'Content-Type: application/json' -H 'Accept: application/json'
curl -i -k -u $1:$2 "http://$3:9091/v1/scopes/project58/streams/logstream2/events" -X PUT -H 'Content-Type: text/plain' -H 'Accept: application/json' -d 'Hello' 
curl -i -k -u $1:$2  "http://$3:9091/v1/scopes/project58/streams/logstream2/segments/0/events?scopeName=project58&streamName=logstream2&segmentNumber=0L&routingKey=key"


#HTTP/1.1 201 Created
#Content-Type: application/json
#Content-Length: 25

#{"scopeName":"project58"}

#HTTP/1.1 201 Created
#Content-Type: application/json
#Content-Length: 165

#{"scopeName":"project58","streamName":"logstream2","scalingPolicy":{"type":"FIXED_NUM_SEGMENTS","minSegments":1},"retentionPolicy":{"type":"LIMITED_DAYS","value":1}}

#HTTP/1.1 201 Created
#Content-Type: application/json
#Content-Length: 25

#{"scopeName":"project58"}

#HTTP/1.1 200 OK
#Content-Type: application/json
#Content-Length: 5

#Hello
