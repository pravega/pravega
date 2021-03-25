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

# Pravega Gateways

As part of the Pravega ecosystem, we provide a set of Gateways that can help applications and services to
interact with Pravega:

## gRPC Gateway
[gRPC Gateway](https://github.com/pravega/pravega-grpc-gateway) is a sample [gRPC](https://grpc.io/) server 
that provides a gateway to Pravega. It provides limited Pravega functionality to any environment that support gRPC, 
including Python.

## HTTP Gateway
[HTTP Gateway](https://github.com/pravega/pravega-ingest-gateway) is a simple HTTP server that can be used to write 
JSON events to a Pravega stream.