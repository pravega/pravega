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
# Pravega Client Libraries

## Supported clients
Pravega supports the following client libraries:

- [Java client](javadoc.md)

- [Rust client](https://pravega.github.io/pravega-client-rust/Rust/index.html) ([project](https://github.com/pravega/pravega-client-rust))

- [Python client](https://pravega.github.io/pravega-client-rust/Python/index.html) ([package](https://pypi.org/project/pravega/))

## Third-party contributions
In addition to the connectors provided by the Pravega organization, open-source contributors have also
created connectors for external projects:

- [.NET client (via gRPC Gateway)](https://github.com/rofr/pravega-sharp) ([package](https://www.nuget.org/packages/PravegaSharp.Grpc))
connects .NET languages to [Pravega gRPC Gateway](gateways.md#grpc-gateway)