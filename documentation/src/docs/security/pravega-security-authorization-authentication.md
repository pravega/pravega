<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# TLS, Authorization, Authentication - enabling encryption, authorization and authentication features
Pravega is used to store critical data for customers. 
The goal is to make it possible to run Pravega as a central streaming storage for different parts of the organizations.
Having different levels of access to the data. This feature is crucial to supprt multi tenancy.

Following key features are added as part of security implementation:
1. Administrators can enable encryption for different communication channels using TLS.
2. A Unix-like permission system which controls which users can access which part of the data and the levels of access.
3. Drop in implementations of the authorization/authentication APIs can be dropped in and loaded dynamically. See [here](../auth/auth-plugin.md) for more details.
Multiple such implementations can co-exist and different users can use different plugins.
4. Users can specify auth parameters to the client through multiple mechanisms. See [here](../auth/client-auth.md) for more details. 
5. All the other compnents that are deployed with Pravega (e.g. Bookkeeper, Zookeeper etc) can be deployed securely with TLS.

[PDP-23](https://github.com/pravega/pravega/wiki/PDP-23:-Pravega-security----encryption-and-Role-Based-Access-Control) 
discusses different options for this design and pros and cons in detail. 