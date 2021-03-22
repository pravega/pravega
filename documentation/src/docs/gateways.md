<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Gateways

As part of the Pravega ecosystem, we provide a set of Gateways that can help applications and services to
interact with Pravega:

- [gRPC Gateway](https://github.com/pravega/pravega-grpc-gateway): This is a sample [gRPC](https://grpc.io/) server 
that provides a gateway to Pravega. It provides limited Pravega functionality to any environment that support gRPC, 
including Python.

- [HTTP Gateway](https://github.com/pravega/pravega-ingest-gateway): Simple HTTP server that can be used to write 
JSON events to a Pravega stream.