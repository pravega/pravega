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
# TLS, Authorization, Authentication - Enabling encryption, authorization and authentication features
Pravega ingests application data, which is often sensitive and requires security mechanisms to avoid 
unauthorized access. To prevent such unauthorized accesses in shared environments, we have enabled 
mechanisms in Pravega that secure Stream data stored in a Pravega cluster. The security documentation 
covers aspects of our mechanisms and provides configuration details to enable security in Pravega.

Key features of security implementation:

1. Pravega allows administrators to enable encryption for different communication channels using TLS.
2. Pravega provides role Based access control which can be availed by a variety of enterprises.
3. Pravega performs dynamic implementations of the Authorization/Authentication [API](../auth/auth-plugin.md). Multiple implementations can co-exist and different plugins can be used by different users.
4. Multiple mechanisms are enabled by Pravega to the users for specifying _auth_ parameters to the client. See [here](../auth/client-auth.md) for more details.
5. Components like Bookkeeper, Zookeeper etc., which are deployed with Pravega can be deployed securely with TLS.

[PDP-23](https://github.com/pravega/pravega/wiki/PDP-23-%28Pravega-Security-Encryption-and-Role-Based-Access-Control%29)
discusses various options for this design and anlayzes the pros and cons in detail.
